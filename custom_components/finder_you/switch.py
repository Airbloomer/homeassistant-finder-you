"""Switch entity for Finder YOU smart plugs and sockets."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.switch import (
    SwitchDeviceClass,
    SwitchEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import Switch
from .const import DOMAIN
from .coordinator import FinderYouCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: FinderYouCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        FinderYouSwitch(coordinator, switch) for switch in coordinator.switches
    )


class FinderYouSwitch(CoordinatorEntity[FinderYouCoordinator], SwitchEntity, RestoreEntity):
    """Finder YOU Switch entity (smart plug / socket)."""

    _attr_has_entity_name = True
    _attr_device_class = SwitchDeviceClass.OUTLET

    def __init__(self, coordinator: FinderYouCoordinator, switch: Switch) -> None:
        super().__init__(coordinator)
        self._switch = switch
        self._attr_unique_id = f"{DOMAIN}_switch_{switch.uuid}_{switch.channel}"
        self._attr_name = None

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{switch.uuid}_{switch.channel}")},
            name=switch.name,
            manufacturer="Finder",
            model=switch.device_model,
        )
        self._is_on: bool = False

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        # Seed from live coordinator channel state first if available
        channel_data = self.coordinator.channel_states.get((self._switch.uuid, self._switch.channel))
        if channel_data is not None and "is_on" in channel_data:
            self._is_on = channel_data["is_on"]
            return

        last = await self.async_get_last_state()
        if last is not None:
            self._is_on = (last.state == "on")

    @property
    def available(self) -> bool:
        """Return True if entity is available.

        Stays available if local BLE routing is active or if coordinator is healthy,
        preventing switches from becoming unavailable during brief cloud gRPC drops.
        """
        if self.coordinator.enable_local_ble or (self._switch.uuid, self._switch.channel) in self.coordinator.channel_states:
            return True
        return super().available

    @property
    def is_on(self) -> bool:
        channel_data = self.coordinator.channel_states.get((self._switch.uuid, self._switch.channel))
        if channel_data is not None and "is_on" in channel_data:
            return channel_data["is_on"]
        return self._is_on


    @property
    def is_syncing(self) -> bool:
        """Return True if a command is currently syncing or verifying."""
        return self.coordinator.is_channel_syncing(self._switch.uuid, self._switch.channel)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {
            "syncing": self.is_syncing,
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the switch."""
        self._is_on = True
        self.async_write_ha_state()
        await self.coordinator.async_switch_on(
            self._switch.uuid, self._switch.channel
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the switch."""
        self._is_on = False
        self.async_write_ha_state()
        await self.coordinator.async_switch_off(
            self._switch.uuid, self._switch.channel
        )
