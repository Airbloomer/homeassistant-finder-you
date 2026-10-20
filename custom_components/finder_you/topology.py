"""Topology and dynamic routing for Finder YESLY BLE mesh nodes.

Tracks link quality (RSSI) of all Finder YESLY devices seen by ESPHome nodes
distributed throughout the home, and routes local commands through the best hop.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from homeassistant.core import Event, HomeAssistant, callback

if TYPE_CHECKING:
    from .api import BleDirectDevice

_LOGGER = logging.getLogger(__name__)

# RSSI considered strong enough for direct transmission
EXCELLENT_RSSI_THRESHOLD = -75
# Maximum age in seconds before an RSSI observation is considered stale
TOPOLOGY_STALE_TIMEOUT = 300.0
# Guard duration in seconds to ignore delayed or stale mesh telemetry after a local transmission
BLE_TX_GUARD_DURATION = 6.0

# Physical room adjacency order for fallback proxy selection when RSSI is unavailable
NODE_ADJACENCY_MAP: dict[str, list[str]] = {
    "esphome_web_cef3a0": [
        "bluetooth_proxy_edge",
        "bluetooth_proxy_sara",
        "bluetooth_proxy_rack_183048",
        "esphome_web_ce31c4",
        "esp32_bluetooth_proxy_18264c",
    ],
    "bluetooth_proxy_edge": [
        "esphome_web_cef3a0",
        "bluetooth_proxy_sara",
        "bluetooth_proxy_rack_183048",
        "esp32_bluetooth_proxy_18264c",
        "esphome_web_ce31c4",
    ],
    "bluetooth_proxy_sara": [
        "bluetooth_proxy_edge",
        "esphome_web_cef3a0",
        "bluetooth_proxy_rack_183048",
        "esp32_bluetooth_proxy_18264c",
        "esphome_web_ce31c4",
    ],
    "bluetooth_proxy_rack_183048": [
        "esphome_web_cef3a0",
        "bluetooth_proxy_sara",
        "bluetooth_proxy_edge",
        "esp32_bluetooth_proxy_18264c",
        "esphome_web_ce31c4",
    ],
    "esphome_web_ce31c4": [
        "esp32_bluetooth_proxy_18264c",
        "bluetooth_proxy_rack_183048",
        "esphome_web_cef3a0",
        "bluetooth_proxy_sara",
        "bluetooth_proxy_edge",
    ],
    "esp32_bluetooth_proxy_18264c": [
        "esphome_web_ce31c4",
        "bluetooth_proxy_rack_183048",
        "esphome_web_cef3a0",
        "bluetooth_proxy_sara",
        "bluetooth_proxy_edge",
    ],
}

# Mapping of known ESPHome proxy Bluetooth/WiFi MAC prefixes to ESPHome service node names
SOURCE_MAC_TO_NODE_MAP: dict[str, str] = {
    "A8:42:E3:CE:31:C6": "esphome_web_ce31c4",
    "A8:42:E3:CE:31:C4": "esphome_web_ce31c4",
    "A8:42:E3:CD:76:F2": "bluetooth_proxy_sara",
    "A8:42:E3:CD:76:F0": "bluetooth_proxy_sara",
    "A8:42:E3:CD:24:8E": "bluetooth_proxy_edge",
    "A8:42:E3:CD:24:8C": "bluetooth_proxy_edge",
    "30:AE:A4:18:30:4A": "bluetooth_proxy_rack_183048",
    "30:AE:A4:18:30:48": "bluetooth_proxy_rack_183048",
    "A8:42:E3:CE:F3:A2": "esphome_web_cef3a0",
    "A8:42:E3:CE:F3:A0": "esphome_web_cef3a0",
    "30:AE:A4:18:26:4E": "esp32_bluetooth_proxy_18264c",
    "30:AE:A4:18:26:4C": "esp32_bluetooth_proxy_18264c",
}


class FinderTopologyRouter:
    """Tracks ESPHome node visibility and selects the best transmitter for each device."""

    def __init__(self, hass: HomeAssistant, coordinator: Any = None) -> None:
        self.hass = hass
        self._coordinator = coordinator
        # device_mac -> {node_name: {"rssi": int, "last_seen": float, "mesh_hop": bool}}
        self._topology: dict[str, dict[str, dict[str, Any]]] = {}
        # Set of active ESPHome node names discovered on the network
        self._esphome_nodes: set[str] = set()
        # device_uuid -> timestamp until which conflicting telemetry updates are ignored
        self._tx_guard_until: dict[str, float] = {}
        # Deduplication cache for identical raw BLE payloads across dual listeners: mac -> (payload_tuple, ts)
        self._last_decoded_adv: dict[str, tuple[tuple[int, ...], float]] = {}
        self._unsub_listener = None
        self._unsub_bt_callback = None

    def start(self) -> None:
        """Start listening for BLE advertisement events and register known nodes."""
        self._install_bleak_finder_guard()
        self._unsub_listener = self.hass.bus.async_listen(
            "esphome.finder_ble_adv", self._handle_ble_adv_event
        )
        self._register_ha_bluetooth_listener()
        self._discover_registered_nodes()
        _LOGGER.info("Finder BLE Topology Router started")

    def stop(self) -> None:
        """Stop listening to events."""
        if self._unsub_listener:
            self._unsub_listener()
            self._unsub_listener = None
        if self._unsub_bt_callback:
            try:
                self._unsub_bt_callback()
            except Exception:
                pass
            self._unsub_bt_callback = None

    def _install_bleak_finder_guard(self) -> None:
        """Prevent HA Bluetooth from opening active GATT connections to connectionless Finder YESLY (05:61:*) devices.

        Finder YESLY actuators (13.S2, 13.72, 15.71) advertise as connectable peripherals without a GAP name,
        which causes habluetooth / active proxies to invoke BleakClient.connect() right after a shutter starts
        moving. While a 13.S2 actuator is held in an active GATT session, it stops broadcasting state packets
        and ignores connectionless BLE mesh commands.
        """
        try:
            import traceback
            from bleak.exc import BleakError
            from habluetooth.wrappers import HaBleakClientWrapper

            orig_connect = getattr(HaBleakClientWrapper, "connect", None)
            if orig_connect is None:
                return
            if getattr(orig_connect, "_finder_yesly_guarded", False):
                orig_connect = getattr(orig_connect, "_finder_yesly_orig", orig_connect)

            logged_caller = False

            async def _guarded_connect(client_self: Any, *args: Any, **kwargs: Any) -> bool:
                nonlocal logged_caller
                raw_addr = (
                    getattr(client_self, "_HaBleakClientWrapper__address", None)
                    or getattr(client_self, "_address", None)
                    or getattr(client_self, "address", None)
                    or ""
                )
                addr = str(raw_addr).upper().replace("-", ":")
                if addr.startswith("05:61:"):
                    if not logged_caller:
                        logged_caller = True
                        stack_str = "".join(traceback.format_stack(limit=10))
                        _LOGGER.info(
                            "Blocked active GATT connection attempt to Finder YESLY device %s. Caller stack:\n%s",
                            addr,
                            stack_str,
                        )
                    else:
                        _LOGGER.debug(
                            "Blocked active GATT connection attempt to connectionless Finder YESLY device %s",
                            addr,
                        )
                    raise BleakError(
                        f"Active GATT connections are disabled for connectionless Finder YESLY device {addr}"
                    )
                return await orig_connect(client_self, *args, **kwargs)

            setattr(_guarded_connect, "_finder_yesly_guarded", True)
            setattr(_guarded_connect, "_finder_yesly_orig", orig_connect)
            HaBleakClientWrapper.connect = _guarded_connect  # type: ignore[method-assign]
            _LOGGER.info("Installed BLE GATT connection guard for Finder YESLY (05:61:*) devices")
        except Exception as err:
            _LOGGER.debug("Could not install BLE GATT guard: %s", err)

    def _register_ha_bluetooth_listener(self) -> None:
        """Register passive listener on HA's native bluetooth integration to capture 05:61:* advertisements from all proxies (including rc-switch)."""
        try:
            from homeassistant.components import bluetooth
            from homeassistant.components.bluetooth.match import BluetoothCallbackMatcher

            self._unsub_bt_callback = bluetooth.async_register_callback(
                self.hass,
                self._handle_ha_bluetooth_adv,
                BluetoothCallbackMatcher(connectable=False),
                bluetooth.BluetoothScanningMode.PASSIVE,
            )
            _LOGGER.info("Registered passive HA Bluetooth callback for Finder YESLY telemetry")
        except Exception as err:
            _LOGGER.debug("Could not register passive HA Bluetooth callback: %s", err)

    def _resolve_source_mac_to_node(self, source: str | None) -> str | None:
        """Map an HA Bluetooth scanner source MAC to a registered ESPHome finder_mesh node."""
        if not source:
            return None
        src_clean = source.upper().replace("-", ":")
        mapped = SOURCE_MAC_TO_NODE_MAP.get(src_clean)
        if mapped and mapped in self._esphome_nodes:
            return mapped
        return None

    @callback
    def _handle_ha_bluetooth_adv(self, service_info: Any, change: Any) -> None:
        """Handle BLE advertisements captured by any HA Bluetooth proxy (including standard proxies like rc-switch)."""
        mac = getattr(service_info, "address", None)
        if not mac:
            return
        mac_clean = mac.upper().replace("-", ":")
        if not mac_clean.startswith("05:61:"):
            return

        mf_data = getattr(service_info, "manufacturer_data", None) or {}
        if not mf_data:
            return

        raw_bytes = mf_data.get(0x01D9) or mf_data.get(0x08F2)
        if raw_bytes is None:
            raw_bytes = next(iter(mf_data.values()), None)
        if not raw_bytes:
            return

        rssi = getattr(service_info, "rssi", None)
        rssi_int = int(rssi) if isinstance(rssi, (int, float)) else -95
        source = getattr(service_info, "source", None)
        node = self._resolve_source_mac_to_node(source)

        self._process_finder_adv_bytes(
            mac_clean=mac_clean,
            rssi=rssi_int,
            byte_list=list(raw_bytes),
            node=node,
            source_label=node or source or "ha_bt",
        )

    def _discover_registered_nodes(self) -> None:
        """Scan HA services to find ESPHome nodes that expose finder_mesh services."""
        services = self.hass.services.async_services()
        esphome_services = services.get("esphome", {})
        for s_name in esphome_services:
            if "finder_mesh" in s_name:
                parts = s_name.split("_finder_mesh_")
                if len(parts) == 2:
                    node_name = parts[0]
                    if node_name not in self._esphome_nodes:
                        self._esphome_nodes.add(node_name)
                        _LOGGER.info("Discovered compatible ESPHome mesh node: %s", node_name)

    def _resolve_esphome_node(self, node: str | None, device_id: str | None) -> str | None:
        """Resolve the ESPHome node name from an explicit node name or device_id."""
        if node and node in self._esphome_nodes:
            return node

        if not device_id:
            return None

        # Check in-memory cache first
        if not hasattr(self, "_dev_id_to_node"):
            self._dev_id_to_node: dict[str, str | None] = {}

        if device_id in self._dev_id_to_node:
            cached = self._dev_id_to_node[device_id]
            if cached and cached in self._esphome_nodes:
                return cached

        # Look up device in HA device registry
        try:
            from homeassistant.helpers import device_registry as dr
            dev_reg = dr.async_get(self.hass)
            dev_entry = dev_reg.async_get(device_id)
            if dev_entry:
                raw_name = (dev_entry.name or dev_entry.name_by_user or "").lower()
                area_id = (dev_entry.area_id or "").lower()
                combined = f"{raw_name}_{area_id}".replace("-", "_").replace(" ", "_")

                # Keyword-to-node alias mapping
                alias_map = {
                    "cucina": "esphome_web_ce31c4",
                    "ce31c4": "esphome_web_ce31c4",
                    "camera_media": "esphome_web_cef3a0",
                    "cef3a0": "esphome_web_cef3a0",
                    "sala": "esp32_bluetooth_proxy_18264c",
                    "soggiorno": "esp32_bluetooth_proxy_18264c",
                    "18264c": "esp32_bluetooth_proxy_18264c",
                    "rack": "bluetooth_proxy_rack_183048",
                    "antibagno": "bluetooth_proxy_rack_183048",
                    "183048": "bluetooth_proxy_rack_183048",
                    "sara": "bluetooth_proxy_sara",
                    "edge": "bluetooth_proxy_edge",
                }

                matched_node = None
                for kw, target_node in alias_map.items():
                    if kw in combined and target_node in self._esphome_nodes:
                        matched_node = target_node
                        break

                if not matched_node:
                    for registered in self._esphome_nodes:
                        reg_clean = registered.lower().replace("-", "_")
                        if reg_clean in combined or any(
                            tok in combined
                            for tok in reg_clean.split("_")
                            if tok not in ("bluetooth", "proxy", "esphome", "web", "esp32")
                        ):
                            matched_node = registered
                            break

                self._dev_id_to_node[device_id] = matched_node
                if matched_node:
                    _LOGGER.info(
                        "Resolved ESPHome device_id '%s' (%s, area=%s) -> node '%s'",
                        device_id,
                        dev_entry.name,
                        dev_entry.area_id,
                        matched_node,
                    )
                return matched_node
        except Exception as err:
            _LOGGER.debug("Failed resolving device_id '%s': %s", device_id, err)

        return None

    @callback
    def _handle_ble_adv_event(self, event: Event) -> None:
        """Process incoming BLE advertisement observation from an ESPHome node."""
        data = event.data
        raw_node = data.get("node")
        device_id = data.get("device_id")
        node = self._resolve_esphome_node(raw_node, device_id)
        mac = data.get("mac")
        rssi_str = data.get("rssi")
        mf_str = data.get("mf", "")

        if not mac or not rssi_str:
            return

        try:
            rssi = int(rssi_str)
        except (ValueError, TypeError):
            return

        if node:
            self._esphome_nodes.add(node)
        mac_clean = mac.upper().replace("-", ":")

        # Parse manufacturer hex bytes
        byte_list: list[int] = []
        parts = mf_str.strip().split()
        if parts:
            for p in parts:
                try:
                    byte_list.append(int(p, 16))
                except ValueError:
                    pass

        self._process_finder_adv_bytes(
            mac_clean=mac_clean,
            rssi=rssi,
            byte_list=byte_list,
            node=node,
            source_label=node or "unknown",
        )

    def _process_finder_adv_bytes(
        self,
        mac_clean: str,
        rssi: int,
        byte_list: list[int],
        node: str | None,
        source_label: str = "unknown",
    ) -> None:
        """Common topology and state telemetry processor for Finder YESLY BLE advertisements."""
        is_mesh_relay = False
        if byte_list and (byte_list[0] & 0x40) != 0:
            is_mesh_relay = True

        now = time.time()
        if node:
            node_entry = self._topology.setdefault(mac_clean, {}).setdefault(node, {})
            node_entry["rssi"] = rssi
            node_entry["last_seen"] = now
            node_entry["is_mesh_relay"] = is_mesh_relay

        if is_mesh_relay:
            _LOGGER.debug(
                "Ignoring mesh relay frame (0x4x, %d bytes) from repeater %s via '%s'",
                len(byte_list),
                mac_clean,
                source_label,
            )
            return

        _LOGGER.debug(
            "BLE Topology updated: MAC %s seen by '%s' (RSSI: %d dBm)",
            mac_clean,
            source_label,
            rssi,
        )

        # Telemetry decoding: match direct frame by exact MAC with device inventory in coordinator
        if self._coordinator and hasattr(self._coordinator, "ble_inventory"):
            inv = self._coordinator.ble_inventory
            matched_dev = None

            for dev in inv.values():
                if dev.mac.upper().replace("-", ":") == mac_clean:
                    matched_dev = dev
                    break

            if matched_dev:
                # Do not interpret command broadcast frames as state telemetry!
                # ESPHome / Gateway command packets are 9 bytes: [4B Counter] [1B Cmd] [4B AES-CCM MIC].
                # Decompiled BleDeviceSupport.GetDeviceChannelStatus requires data.Length >= 11 for telemetry!
                if len(byte_list) < 11:
                    _LOGGER.debug(
                        "Ignoring short/command BLE packet (%d bytes) for %s from '%s'",
                        len(byte_list),
                        matched_dev.mac,
                        source_label,
                    )
                    return

                if node:
                    self._topology.setdefault(mac_clean, {}).setdefault(node, {})["last_telemetry"] = now

                # Deduplicate identical raw frame within 200ms across dual listeners
                payload_key = tuple(byte_list[:10])
                prev_dec = self._last_decoded_adv.get(mac_clean)
                if prev_dec is not None and prev_dec[0] == payload_key and (now - prev_dec[1]) < 0.2:
                    return
                self._last_decoded_adv[mac_clean] = (payload_key, now)

                channel_states: dict[int, bool] = {}
                percent_val: int | None = None
                fam = (matched_dev.family or "").upper()
                is_dimmer = any(k in fam for k in ["1521", "1571"])
                is_shutter = any(k in fam for k in ["13S2", "SHUTTER"])

                if is_dimmer:
                    if len(byte_list) > 4:
                        raw_pos = byte_list[4]
                        if 0 <= raw_pos <= 100:
                            percent_val = raw_pos
                elif is_shutter:
                    if len(byte_list) > 5:
                        raw_pos = byte_list[5]
                        if 0 <= raw_pos <= 100:
                            percent_val = raw_pos
                else:
                    status_byte: int | None = byte_list[6] if len(byte_list) >= 7 else None
                    if status_byte is not None:
                        channel_states[1] = bool(status_byte & 0x01)
                        channel_states[2] = bool(status_byte & 0x10)

                if channel_states or percent_val is not None:
                    self._coordinator.handle_ble_device_state_update(
                        matched_dev.uuid,
                        channel_states=channel_states if channel_states else None,
                        percent=percent_val,
                    )

    def has_direct_telemetry(self, device: BleDirectDevice, max_age: float = 1800.0) -> bool:
        """Return True if any active ESPHome node has received direct BLE telemetry from this device."""
        mac_upper = device.mac.upper().replace("-", ":")
        now = time.time()
        candidates = self._topology.get(mac_upper, {})
        for node, info in candidates.items():
            if node in self._esphome_nodes and (now - info.get("last_telemetry", 0)) < max_age:
                return True
        if mac_upper in self._last_decoded_adv and (now - self._last_decoded_adv[mac_clean if (mac_clean := mac_upper) else mac_upper][1]) < max_age:
            return True
        return False

    def get_node_rssi(self, device: BleDirectDevice, node: str | None) -> int | None:
        """Return the last recorded RSSI (in dBm) of `device` on `node`, or None if unknown."""
        if not node:
            return None
        mac_upper = device.mac.upper().replace("-", ":")
        info = self._topology.get(mac_upper, {}).get(node)
        if info and "rssi" in info:
            return int(info["rssi"])
        return None

    def _get_affinity_node(self, device: BleDirectDevice, channel: int | None = None) -> tuple[str | None, str]:
        """Return (affinity_node, matched_keyword) based on device/channel name and room."""
        dev_search_str = ""
        if self._coordinator:
            for l in getattr(self._coordinator, "lights", []):
                if l.uuid == device.uuid and (channel is None or l.channel == channel):
                    dev_search_str += f" {l.name} {l.room or ''}"
            for s in getattr(self._coordinator, "shutters", []):
                if s.uuid == device.uuid:
                    dev_search_str += f" {s.name} {s.room or ''}"
            for sw in getattr(self._coordinator, "switches", []):
                if sw.uuid == device.uuid and (channel is None or sw.channel == channel):
                    dev_search_str += f" {sw.name} {sw.room or ''}"
        dev_search_clean = dev_search_str.lower().replace("-", "_").replace(" ", "_")

        affinity_map = {
            "sara": "bluetooth_proxy_sara",
            "cameretta": "bluetooth_proxy_sara",
            "cucina": "esphome_web_ce31c4",
            "media": "esphome_web_cef3a0",
            "giochi": "esphome_web_cef3a0",
            "studio": "esp32_bluetooth_proxy_18264c",
            "sala": "esp32_bluetooth_proxy_18264c",
            "soggiorno": "esp32_bluetooth_proxy_18264c",
            "pranzo": "esp32_bluetooth_proxy_18264c",
            "ingresso": "esp32_bluetooth_proxy_18264c",
            "corridoio": "esp32_bluetooth_proxy_18264c",
            "bagno": "bluetooth_proxy_rack_183048",
            "lavanderia": "bluetooth_proxy_rack_183048",
            "rack": "bluetooth_proxy_rack_183048",
            "matrimoniale": "bluetooth_proxy_edge",
            "letto": "bluetooth_proxy_edge",
        }
        for kw, aff_node in affinity_map.items():
            if kw in dev_search_clean and aff_node in self._esphome_nodes:
                return aff_node, kw
        return None, ""

    def select_best_node(self, device: BleDirectDevice, channel: int | None = None) -> str | None:
        """Select the best ESPHome node to transmit to the target device."""
        self._discover_registered_nodes()
        if not self._esphome_nodes:
            return None

        mac_upper = device.mac.upper().replace("-", ":")
        now = time.time()

        aff_node, aff_kw = self._get_affinity_node(device, channel=channel)

        candidates = self._topology.get(mac_upper, {})
        valid_candidates: list[tuple[str, int]] = []
        for node, info in candidates.items():
            if node in self._esphome_nodes and (now - info.get("last_seen", 0)) < TOPOLOGY_STALE_TIMEOUT:
                valid_candidates.append((node, info.get("rssi", -100)))

        valid_candidates.sort(key=lambda x: x[1], reverse=True)
        cand_dict = dict(valid_candidates)

        if valid_candidates:
            best_node, best_rssi = valid_candidates[0]
            if aff_node:
                aff_rssi = cand_dict.get(aff_node)
                if best_node == aff_node:
                    _LOGGER.info(
                        "Selected best node '%s' for MAC %s based on room affinity '%s' and direct RSSI (%d dBm)",
                        best_node,
                        mac_upper,
                        aff_kw,
                        best_rssi,
                    )
                    return best_node
                if best_rssi >= -68 and (aff_rssi is None or best_rssi >= aff_rssi + 8):
                    _LOGGER.info(
                        "Selected best node '%s' for MAC %s based on strong direct RSSI (%d dBm, overriding affinity '%s')",
                        best_node,
                        mac_upper,
                        best_rssi,
                        aff_node,
                    )
                    return best_node
                _LOGGER.info(
                    "Selected affinity node '%s' ('%s') for MAC %s over weak cross-room RSSI on '%s' (%d dBm)",
                    aff_node,
                    aff_kw,
                    mac_upper,
                    best_node,
                    best_rssi,
                )
                return aff_node

            _LOGGER.info(
                "Selected best node '%s' for MAC %s based on direct RSSI (%d dBm)",
                best_node,
                mac_upper,
                best_rssi,
            )
            return best_node

        if aff_node:
            _LOGGER.info(
                "Selected node '%s' for device %s (%s) based on room/name affinity '%s'",
                aff_node,
                device.uuid,
                device.mac,
                aff_kw,
            )
            return aff_node

        # Historical RSSI for stationary wall devices when >5 min idle
        hist_candidates: list[tuple[str, int]] = [
            (node, info.get("rssi", -100))
            for node, info in candidates.items()
            if node in self._esphome_nodes
        ]
        if hist_candidates:
            hist_candidates.sort(key=lambda x: x[1], reverse=True)
            return hist_candidates[0][0]

        # Sighting via mesh repeaters
        mesh_scores: dict[str, int] = {}
        for dev_mac, nodes in self._topology.items():
            for node, info in nodes.items():
                if node in self._esphome_nodes and (now - info.get("last_seen", 0)) < TOPOLOGY_STALE_TIMEOUT:
                    mesh_scores[node] = max(mesh_scores.get(node, -100), info.get("rssi", -100))

        if mesh_scores:
            best_mesh_node = max(mesh_scores.items(), key=lambda x: x[1])[0]
            return best_mesh_node

        fallback_node = next(iter(self._esphome_nodes))
        return fallback_node

    def select_alternate_node(
        self,
        device: BleDirectDevice,
        exclude_node: str | None,
        channel: int | None = None,
    ) -> str | None:
        """Select the best alternate ESPHome node excluding `exclude_node`."""
        self._discover_registered_nodes()
        available = [n for n in self._esphome_nodes if n != exclude_node]
        if not available:
            return None

        aff_node, _ = self._get_affinity_node(device, channel=channel)
        if aff_node and aff_node in available:
            return aff_node

        mac_upper = device.mac.upper().replace("-", ":")
        now = time.time()
        candidates = self._topology.get(mac_upper, {})

        # 1. Fresh RSSI candidates (< 5 min)
        valid_candidates: list[tuple[str, int]] = []
        for node, info in candidates.items():
            if node in available and (now - info.get("last_seen", 0)) < TOPOLOGY_STALE_TIMEOUT:
                valid_candidates.append((node, info.get("rssi", -100)))
        if valid_candidates:
            valid_candidates.sort(key=lambda x: x[1], reverse=True)
            return valid_candidates[0][0]

        # 2. Historical RSSI candidates (stationary wall-mounted shutters/relays)
        hist_candidates: list[tuple[str, int]] = [
            (node, info.get("rssi", -100))
            for node, info in candidates.items()
            if node in available and "rssi" in info
        ]
        if hist_candidates:
            hist_candidates.sort(key=lambda x: x[1], reverse=True)
            return hist_candidates[0][0]

        # 3. Physical room adjacency map from primary / affinity node
        ref_node = exclude_node or aff_node
        if ref_node and ref_node in NODE_ADJACENCY_MAP:
            for neighbor in NODE_ADJACENCY_MAP[ref_node]:
                if neighbor in available:
                    return neighbor

        if "esp32_bluetooth_proxy_18264c" in available:
            return "esp32_bluetooth_proxy_18264c"
        return available[0]

    async def async_dispatch_click(
        self,
        device: BleDirectDevice,
        channel: int,
        preferred_node: str | None = None,
    ) -> bool:
        """Transmit a relay / button click via the best ESPHome node or alternate node."""
        self._discover_registered_nodes()
        if not self._esphome_nodes:
            _LOGGER.warning("No ESPHome mesh node available to send click to %s", device.mac)
            return False

        payload = {
            "mac": device.mac,
            "key": device.key,
            "channel": channel,
        }

        target_node = preferred_node or self.select_best_node(device, channel=channel)

        if target_node:
            service_name = f"{target_node}_finder_mesh_click"
            try:
                _LOGGER.info(
                    "Local BLE Dispatch: Calling esphome.%s for MAC %s Ch %d",
                    service_name,
                    device.mac,
                    channel,
                )
                await self.hass.services.async_call("esphome", service_name, payload, blocking=True)
                self._tx_guard_until[device.uuid] = time.time() + BLE_TX_GUARD_DURATION
                return True
            except Exception as err:
                _LOGGER.warning(
                    "Failed to send local BLE click via node '%s': %s", target_node, err
                )

        alt_node = self.select_alternate_node(device, exclude_node=target_node, channel=channel)
        if alt_node:
            alt_service = f"{alt_node}_finder_mesh_click"
            try:
                _LOGGER.info(
                    "Retrying local BLE click via alternate node '%s'", alt_node
                )
                await self.hass.services.async_call("esphome", alt_service, payload, blocking=True)
                self._tx_guard_until[device.uuid] = time.time() + BLE_TX_GUARD_DURATION
                return True
            except Exception as alt_err:
                _LOGGER.warning("Alternate node '%s' also failed: %s", alt_node, alt_err)
        return False

    async def async_dispatch_set_percent(
        self,
        device: BleDirectDevice,
        percent: int,
        preferred_node: str | None = None,
    ) -> bool:
        """Transmit a dimmer or shutter position via the best ESPHome node."""
        target_node = preferred_node or self.select_best_node(device)
        if not target_node:
            _LOGGER.warning("No ESPHome mesh node available to set percent on %s", device.mac)
            return False

        service_name = f"{target_node}_finder_mesh_set_percent"
        payload = {
            "mac": device.mac,
            "key": device.key,
            "percent": percent,
        }
        try:
            _LOGGER.info(
                "Local BLE Dispatch: Calling esphome.%s for MAC %s Percent %d%%",
                service_name,
                device.mac,
                percent,
            )
            await self.hass.services.async_call("esphome", service_name, payload, blocking=True)
            self._tx_guard_until[device.uuid] = time.time() + BLE_TX_GUARD_DURATION
            return True
        except Exception as err:
            _LOGGER.warning(
                "Failed to send local BLE percent via node '%s': %s", target_node, err
            )
            alt_node = self.select_alternate_node(device, exclude_node=target_node)
            if alt_node:
                alt_service = f"{alt_node}_finder_mesh_set_percent"
                try:
                    _LOGGER.info(
                        "Retrying local BLE percent via alternate node '%s'", alt_node
                    )
                    await self.hass.services.async_call("esphome", alt_service, payload, blocking=True)
                    self._tx_guard_until[device.uuid] = time.time() + BLE_TX_GUARD_DURATION
                    return True
                except Exception as alt_err:
                    _LOGGER.warning("Alternate node '%s' also failed: %s", alt_node, alt_err)
            return False

    async def async_dispatch_shutter_stop(
        self,
        device: BleDirectDevice,
        channel: int = 1,
        preferred_node: str | None = None,
    ) -> bool:
        """Transmit a shutter stop command via the best ESPHome node."""
        target_node = preferred_node or self.select_best_node(device)
        if not target_node:
            return False

        service_name = f"{target_node}_finder_mesh_shutter_stop"
        payload = {
            "mac": device.mac,
            "key": device.key,
            "channel": channel,
        }
        try:
            _LOGGER.info(
                "Local BLE Dispatch: Calling esphome.%s for MAC %s Ch %d",
                service_name,
                device.mac,
                channel,
            )
            await self.hass.services.async_call("esphome", service_name, payload, blocking=True)
            return True
        except Exception as err:
            _LOGGER.warning(
                "Failed to send local BLE stop via node '%s': %s", target_node, err
            )
            alt_node = self.select_alternate_node(device, exclude_node=target_node)
            if alt_node:
                alt_service = f"{alt_node}_finder_mesh_shutter_stop"
                try:
                    _LOGGER.info(
                        "Retrying local BLE stop via alternate node '%s'", alt_node
                    )
                    await self.hass.services.async_call("esphome", alt_service, payload, blocking=True)
                    return True
                except Exception as alt_err:
                    _LOGGER.warning("Alternate node '%s' stop also failed: %s", alt_node, alt_err)
            return False
