"""Cover entity for each Finder YOU roller shutter."""

from __future__ import annotations

from typing import Any

from homeassistant.components.cover import (
    ATTR_POSITION,
    CoverDeviceClass,
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import GatewayOfflineError, Shutter
from .const import DOMAIN
from .coordinator import FinderYouCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: FinderYouCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(FinderYouCover(coordinator, shutter) for shutter in coordinator.shutters)


class FinderYouCover(CoordinatorEntity[FinderYouCoordinator], CoverEntity, RestoreEntity):
    _attr_has_entity_name = True
    _attr_device_class = CoverDeviceClass.SHUTTER
    _attr_supported_features = (
        CoverEntityFeature.OPEN
        | CoverEntityFeature.CLOSE
        | CoverEntityFeature.STOP
        | CoverEntityFeature.SET_POSITION
    )
    _attr_assumed_state = False


    def __init__(self, coordinator: FinderYouCoordinator, shutter: Shutter) -> None:
        super().__init__(coordinator)
        self._shutter = shutter
        self._attr_unique_id = f"{DOMAIN}_{shutter.uuid}"
        self._attr_name = None

        connections = set()
        ble_dev = coordinator.ble_inventory.get(shutter.uuid)
        if ble_dev and ble_dev.mac:
            connections.add((dr.CONNECTION_BLUETOOTH, ble_dev.mac.lower()))

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, shutter.uuid)},
            connections=connections,
            name=shutter.name,
            manufacturer="Finder",
            model="YESLY Roller Shutter",
            suggested_area=shutter.room,
        )
        self._last_commanded_position: int | None = None

    async def async_added_to_hass(self) -> None:
        """Restore the last position from before HA restarted."""
        await super().async_added_to_hass()
        last = await self.async_get_last_state()
        if last is not None:
            pos = last.attributes.get("current_position")
            if isinstance(pos, (int, float)):
                self._last_commanded_position = int(pos)

    @property
    def current_cover_position(self) -> int | None:
        """Return observed position from coordinator, falling back to restored state."""
        observed = self.coordinator.data.get(self._shutter.uuid) if self.coordinator.data else None
        if observed is not None:
            return observed
        return self._last_commanded_position


    @property
    def available(self) -> bool:
        """Return True if entity is available.

        Stays available if local BLE routing is active or if coordinator is healthy,
        preventing entities from becoming unavailable during brief cloud gRPC drops.
        """
        if self.coordinator.enable_local_ble or self._last_commanded_position is not None:
            return True
        return super().available

    @property
    def is_closed(self) -> bool | None:
        pos = self.current_cover_position
        if pos is None:
            return None
        return pos == 0

    async def _send(self, op, target_position: int) -> None:
        self._last_commanded_position = target_position
        try:
            await op()
        except GatewayOfflineError as err:
            # The coordinator waited up to VERIFY_TIMEOUT for the
            # gateway's plant cache to reflect the command and never
            # saw a change. Surface the failure to HA so HomeKit reverts
            # to the actual observed state instead of falsely claiming
            # success.
            raise HomeAssistantError(
                f"YESLY gateway didn't acknowledge the {self._shutter.name} "
                f"command ({err}). Try power-cycling the gateway if this persists."
            ) from err
        # Update with real observed position from coordinator if present
        observed = self.coordinator.data.get(self._shutter.uuid) if self.coordinator.data else None
        if observed is not None:
            self._last_commanded_position = observed
        else:
            self._last_commanded_position = target_position
        self.async_write_ha_state()


    async def async_open_cover(self, **kwargs: Any) -> None:
        await self._send(lambda: self.coordinator.async_open(self._shutter.uuid), 100)

    async def async_close_cover(self, **kwargs: Any) -> None:
        await self._send(lambda: self.coordinator.async_close_shutter(self._shutter.uuid), 0)

    async def async_stop_cover(self, **kwargs: Any) -> None:
        await self.coordinator.async_stop_shutter(self._shutter.uuid)

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        pos = kwargs[ATTR_POSITION]
        await self._send(lambda: self.coordinator.async_set_position(self._shutter.uuid, pos), pos)
