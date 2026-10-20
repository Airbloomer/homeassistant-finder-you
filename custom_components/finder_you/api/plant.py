"""Decode the GetPlant response into structured shutter metadata.

The plant payload is ~27 KB of nested protobuf. We only need the list of
roller shutters: their UUID (used by SetOpenPercent) and their display
name.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from .proto import parse_fields

SHUTTER_TYPE_MARKER = b"device_roller_shutter_50"

# Per-shutter state submessage shape: the GetPlant payload contains one
# device-state message per shutter, attached at depth 1 under field 12
# of the plant wrapper. Each carries the shutter UUID at #1, plant UUID
# at #2, MAC at #4, a config JSON blob at #6, last-update timestamp at
# #7, gateway UUID at #8, signal at #9, motion flag at #12 (varint 2 =
# idle, 3 = moving), and the open percentage at #13.#1. The field-id set
# is stable, so we can identify these messages by their schema.
_STATE_FIELD_SET = frozenset({1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13})


@dataclass(frozen=True)
class Shutter:
    """A roller shutter discovered in the plant."""

    uuid: str
    name: str
    room: str | None = None


@dataclass(frozen=True)
class Scenario:
    """A cloud scenario / macro discovered in the plant."""

    uuid: str
    name: str


@dataclass(frozen=True)
class Light:
    """A light or dimmer entity discovered in the plant."""

    uuid: str
    name: str
    channel: int
    is_dimmer: bool
    elem_id: str
    device_model: str = "YESLY Light"
    room: str | None = None


@dataclass(frozen=True)
class Switch:
    """A switch/socket entity discovered in the plant."""

    uuid: str
    name: str
    channel: int
    elem_id: str
    device_model: str = "YESLY Switch"
    room: str | None = None


@dataclass(frozen=True)
class BleDirectDevice:
    """A device ready for direct local BLE mesh communication."""

    uuid: str
    mac: str
    family: str
    key: str  # AES-128 hex string (32 chars)


def extract_ble_inventory(plant_payload: bytes) -> dict[str, BleDirectDevice]:
    """Extract all BLE-capable devices with their MAC and Master AES-128 key."""
    try:
        top = parse_fields(plant_payload)
        if 3 not in top or not isinstance(top[3][0], bytes):
            return {}
        plant = parse_fields(top[3][0])

        hw_map: dict[str, dict[str, str]] = {}
        for item in plant.get(12, []):
            if not isinstance(item, bytes):
                continue
            sub = parse_fields(item)
            u_b = sub.get(1, [b""])[0]
            fam_b = sub.get(3, [b""])[0]
            f4 = sub.get(4, [b""])[0]
            mac = ""
            if isinstance(f4, bytes):
                mac = parse_fields(f4).get(1, [b""])[0].decode(errors="ignore")
            u = u_b.decode(errors="ignore") if isinstance(u_b, bytes) else ""
            fam = fam_b.decode(errors="ignore") if isinstance(fam_b, bytes) else ""
            if u:
                hw_map[u] = {"mac": mac, "family": fam}

        keys_map: dict[str, str] = {}
        for item in plant.get(11, []):
            if not isinstance(item, bytes):
                continue
            sub = parse_fields(item)
            k_type = sub.get(2, [b""])[0].decode(errors="ignore")
            f3 = sub.get(3, [b""])
            target_uuid = ""
            if f3 and isinstance(f3[0], bytes):
                target_uuid = parse_fields(f3[0]).get(1, [b""])[0].decode(errors="ignore")
            raw_key = sub.get(7, [b""])[0]
            key_hex = raw_key.hex() if isinstance(raw_key, bytes) else ""
            if target_uuid and key_hex and k_type == "AES128/MASTER":
                keys_map[target_uuid] = key_hex

        inventory: dict[str, BleDirectDevice] = {}
        for u, hw in hw_map.items():
            if u in keys_map and hw.get("mac"):
                inventory[u] = BleDirectDevice(
                    uuid=u,
                    mac=hw["mac"],
                    family=hw["family"],
                    key=keys_map[u],
                )
        return inventory
    except Exception:
        return {}



def parse_plant(payload: bytes) -> tuple[str, list[Shutter]]:
    """Extract ``(plant_name, [Shutter, ...])`` from a GetPlant response body.

    The plant has many nested messages. Roller shutters live in entries
    that we'll call "rooms"::

        room {                       (field 3 of the outer plant message)
          #1 string room_uuid
          #2 device { #1 string device_uuid }
          #3 varint                  (some flag)
          #4 varint                  (some flag)
          #7 string display_name     (e.g. "Living room", "Bedroom")
          #8 message { #1 string "device_roller_shutter_50" }
          ... other devices (lights) attached to the same room
        }

    We walk the plant looking for messages whose serialized bytes contain
    the shutter type marker, then pull the device UUID + display name out.
    """
    top = parse_fields(payload)
    plant_name = ""
    if 3 in top:
        plant = parse_fields(top[3][0])
        if 2 in plant:
            plant_name = plant[2][0].decode("utf-8", errors="replace")
    shutters: list[Shutter] = []
    _walk(payload, shutters)
    # De-duplicate by UUID (shouldn't happen but be safe).
    seen: set[str] = set()
    unique: list[Shutter] = []
    for s in shutters:
        if s.uuid not in seen:
            seen.add(s.uuid)
            unique.append(s)
    unique.sort(key=lambda s: s.name.lower())
    return plant_name, unique


def parse_scenarios(payload: bytes) -> list[Scenario]:
    """Extract all cloud scenarios from GetPlant payload."""
    try:
        top = parse_fields(payload)
        plant = parse_fields(top[3][0])
        items_msg = plant.get(15, [None])[0]
        if not items_msg or not isinstance(items_msg, bytes):
            return []
        sub = parse_fields(items_msg)
        scenarios: list[Scenario] = []
        for elem in sub.get(3, []):
            if not isinstance(elem, bytes):
                continue
            f = parse_fields(elem)
            type_str = ""
            if 8 in f and isinstance(f[8][0], bytes):
                t = parse_fields(f[8][0])
                type_str = t.get(1, [b""])[0].decode(errors="replace")
            if type_str == "device_scenario":
                name = f.get(7, [b""])[0].decode(errors="replace")
                elem_id = f.get(1, [b""])[0].decode(errors="replace")
                if elem_id and name:
                    scenarios.append(Scenario(uuid=elem_id, name=name))
        scenarios.sort(key=lambda sc: sc.name.lower())
        return scenarios
    except Exception:
        return []


def parse_lights_and_switches(payload: bytes) -> tuple[list[Light], list[Switch]]:
    """Extract all lights and switches from the plant body."""
    try:
        top = parse_fields(payload)
        plant = parse_fields(top[3][0])
        f12 = plant.get(12, [])
        device_fams: dict[str, str] = {}
        for sub in f12:
            if not isinstance(sub, bytes):
                continue
            sf = parse_fields(sub)
            u_b = sf.get(1, [None])[0]
            if isinstance(u_b, bytes):
                fam_b = sf.get(3, [b""])[0]
                device_fams[u_b.decode(errors="replace")] = fam_b.decode(errors="replace")

        items_msg = plant.get(15, [None])[0]
        if not items_msg or not isinstance(items_msg, bytes):
            return [], []
        sub = parse_fields(items_msg)
        lights: list[Light] = []
        switches: list[Switch] = []
        for elem in sub.get(3, []):
            if not isinstance(elem, bytes):
                continue
            rf = parse_fields(elem)
            elem_id = rf.get(1, [b""])[0].decode(errors="replace")
            name = rf.get(7, [b""])[0].decode(errors="replace")
            t8 = rf.get(8, [b""])[0]
            dev_type = ""
            if isinstance(t8, bytes):
                dev_type = parse_fields(t8).get(1, [b""])[0].decode(errors="replace")
            dev_msg = rf.get(2, [b""])[0]
            dev_uuid = ""
            if isinstance(dev_msg, bytes):
                dev_uuid = parse_fields(dev_msg).get(1, [b""])[0].decode(errors="replace")
            channel = rf.get(3, [1])[0]
            if not isinstance(channel, int):
                channel = 1

            fam = device_fams.get(dev_uuid, "")
            model_name = f"YESLY {fam}" if fam else "YESLY Device"

            if dev_type == "device_light_bulb":
                is_dimmer = (fam == "1521")
                lights.append(
                    Light(
                        uuid=dev_uuid,
                        name=name,
                        channel=channel,
                        is_dimmer=is_dimmer,
                        elem_id=elem_id,
                        device_model=model_name,
                    )
                )
            elif dev_type == "device_plug_socket_europe_2":
                switches.append(
                    Switch(
                        uuid=dev_uuid,
                        name=name,
                        channel=channel,
                        elem_id=elem_id,
                        device_model=model_name,
                    )
                )

        lights.sort(key=lambda x: x.name.lower())
        switches.sort(key=lambda x: x.name.lower())
        return lights, switches
    except Exception:
        return [], []



def _walk(payload: bytes, out: list[Shutter]) -> None:
    """Walk the protobuf tree, extracting shutters."""
    try:
        fields = parse_fields(payload)
    except Exception:
        return
    for field_id, values in fields.items():
        for v in values:
            if not isinstance(v, bytes):
                continue
            if SHUTTER_TYPE_MARKER not in v:
                continue
            shutter = _extract(v)
            if shutter is not None:
                out.append(shutter)
            _walk(v, out)


def extract_shutter_states(payload: bytes) -> dict[str, bytes]:
    """Return ``{shutter_uuid: state_submessage_bytes}`` for verification.

    The state submessage contains UUID, position, motion flag, RSSI, and a
    timestamp that updates on every event. Diffing the whole submessage is
    therefore a reliable "did *this* shutter change?" signal that doesn't
    cross-contaminate when other shutters are moving concurrently — that
    was the bug behind the prior whole-payload approach.

    Returns ``{}`` on a malformed payload rather than raising; the caller
    falls back to whole-payload diff in that case.
    """
    out: dict[str, bytes] = {}
    for sub, fields in _iter_state_submessages(payload):
        uuid_b = fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        out[uuid_b.decode()] = sub
    return out


def extract_shutter_positions(payload: bytes) -> dict[str, int]:
    """Return ``{shutter_uuid: open_percent}`` from a GetPlant response."""
    out: dict[str, int] = {}
    for _sub, fields in _iter_state_submessages(payload):
        uuid_b = fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        pos_msg = fields.get(13, [None])[0]
        if not isinstance(pos_msg, bytes):
            continue
        try:
            pos_fields = parse_fields(pos_msg)
        except Exception:
            continue
        pos = pos_fields.get(1, [None])[0]
        if isinstance(pos, int) and 0 <= pos <= 100:
            out[uuid_b.decode()] = pos
    return out


def extract_shutter_durations(payload: bytes) -> dict[str, tuple[float, float]]:
    """Return ``{shutter_uuid: (open_seconds, close_seconds)}`` from the relay's Ch1T1/Ch1T2 JSON config."""
    out: dict[str, tuple[float, float]] = {}
    for _sub, fields in _iter_state_submessages(payload):
        uuid_b = fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        cfg_msg = fields.get(6, [None])[0]
        if not isinstance(cfg_msg, bytes):
            continue
        try:
            cfg_fields = parse_fields(cfg_msg)
            cfg_bytes = cfg_fields.get(1, [None])[0]
            if not isinstance(cfg_bytes, bytes):
                continue
            cfg = json.loads(cfg_bytes.decode("utf-8", errors="ignore"))
        except Exception:
            continue
        if cfg.get("Ch1Function") != 177 and fields.get(3, [b""])[0] != b"13S2":
            continue
        t1 = cfg.get("Ch1T1")
        t2 = cfg.get("Ch1T2")
        open_s = float(t1) if isinstance(t1, (int, float)) and t1 > 0 else 25.0
        close_s = float(t2) if isinstance(t2, (int, float)) and t2 > 0 else open_s
        out[uuid_b.decode()] = (open_s, close_s)
    return out


def extract_channel_states(payload: bytes) -> dict[tuple[str, int], dict[str, Any]]:
    """Return ``{(device_uuid, channel): {'is_on': bool, 'brightness': int|None}}``."""
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for _sub, fields in _iter_state_submessages(payload):
        uuid_b = fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        dev_uuid = uuid_b.decode()
        fam_b = fields.get(3, [b""])[0]
        fam = fam_b.decode(errors="replace") if isinstance(fam_b, bytes) else ""
        f5 = fields.get(5, [b""])[0]
        f13 = fields.get(13, [None])[0]
        f13_p = {}
        if isinstance(f13, bytes):
            try:
                f13_p = parse_fields(f13)
            except Exception:
                pass

        if fam == "1521":  # Dimmer
            brightness_pct = None
            if 1 in f13_p and isinstance(f13_p[1][0], int):
                brightness_pct = f13_p[1][0]
            f11_val = fields.get(11, [None])[0]
            if f11_val == 3:
                is_on = True
            elif f11_val == 2:
                is_on = False
            else:
                is_on = bool(brightness_pct is not None and brightness_pct > 0)
            brightness_val = (
                round((brightness_pct / 100) * 255) if brightness_pct is not None else None
            )
            out[(dev_uuid, 1)] = {"is_on": is_on, "brightness": brightness_val}
        elif fam in ("1322", "1372"):  # 2-channel Relays / Switches
            f11_val = fields.get(11, [None])[0]
            f12_val = fields.get(12, [None])[0]
            ch1_on = None
            ch2_on = None
            if f11_val is not None:
                ch1_on = (f11_val == 3)
            if f12_val is not None:
                ch2_on = (f12_val == 3)

            # Fallback to f5 status byte if f11/f12 are absent
            if (ch1_on is None or ch2_on is None) and isinstance(f5, bytes) and len(f5) >= 7:
                # In telemetry payload, byte 6 contains relay bits: Ch1 = bit 0, Ch2 = bit 4
                sb = f5[6]
                if ch1_on is None:
                    ch1_on = bool(sb & 0x01)
                if ch2_on is None:
                    ch2_on = bool(sb & 0x10)

            out[(dev_uuid, 1)] = {"is_on": ch1_on if ch1_on is not None else False, "brightness": None}
            out[(dev_uuid, 2)] = {"is_on": ch2_on if ch2_on is not None else False, "brightness": None}
    return out


def extract_shutter_motion(payload: bytes) -> dict[str, int]:
    """Return ``{shutter_uuid: motion_flag}`` from a GetPlant response.

    The motion flag (state submessage field #12) reads 2 when the shutter
    is idle and 3 while the gateway is actively driving the motor over
    BLE-mesh. Observing 3 even briefly during a verify window is proof
    that the BLE-mesh hop reached the shutter — useful when the position
    field is empty (the gateway hasn't yet received a fresh telemetry
    reading from the shutter) and ``extract_shutter_positions`` can't
    tell us whether the motor ran.
    """
    out: dict[str, int] = {}
    for _sub, fields in _iter_state_submessages(payload):
        uuid_b = fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        motion = fields.get(12, [None])[0]
        if isinstance(motion, int):
            out[uuid_b.decode()] = motion
    return out


def _iter_state_submessages(payload: bytes):
    """Yield ``(submessage_bytes, parsed_fields_dict)`` for each device state submessage.

    The per-device state messages live inside the plant body (top-level
    field 3), attached as repeated entries on field 12 of that body.
    Each device state message carries the device UUID as a 36-char string
    at field 1 and family at field 3.
    """
    try:
        top = parse_fields(payload)
    except Exception:
        return
    plant_body = top.get(3, [None])[0]
    if not isinstance(plant_body, bytes):
        return
    try:
        plant_fields = parse_fields(plant_body)
    except Exception:
        return
    for sub in plant_fields.get(12, []):
        if not isinstance(sub, bytes):
            continue
        try:
            sub_fields = parse_fields(sub)
        except Exception:
            continue
        uuid_b = sub_fields.get(1, [None])[0]
        if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
            continue
        yield sub, sub_fields


def _extract(room_bytes: bytes) -> Shutter | None:
    """Pull a single shutter (uuid, name) out of a 'room' submessage."""
    if SHUTTER_TYPE_MARKER not in room_bytes:
        return None
    try:
        fields = parse_fields(room_bytes)
    except Exception:
        return None
    # Display name = field 7 (string).
    name_b = fields.get(7, [None])[0]
    if not isinstance(name_b, bytes):
        return None
    name = name_b.decode("utf-8", errors="replace")
    # Field 8 holds the type marker — confirm it's the shutter marker (so we
    # don't pick up sibling light entries that happen to live in the same
    # parent room).
    type_msg = fields.get(8, [None])[0]
    if not isinstance(type_msg, bytes):
        return None
    type_sub = parse_fields(type_msg)
    if type_sub.get(1, [None])[0] != SHUTTER_TYPE_MARKER:
        return None
    # Device UUID = field 2's field 1 (string).
    device_msg = fields.get(2, [None])[0]
    if not isinstance(device_msg, bytes):
        return None
    device_sub = parse_fields(device_msg)
    uuid_b = device_sub.get(1, [None])[0]
    if not isinstance(uuid_b, bytes) or len(uuid_b) != 36:
        return None
    return Shutter(uuid=uuid_b.decode(), name=name)
