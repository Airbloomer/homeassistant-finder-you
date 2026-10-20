"""Light entity for Finder YOU dimmers and relays."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ColorMode,
    LightEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import Light
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
        FinderYouLight(coordinator, light) for light in coordinator.lights
    )


class FinderYouLight(CoordinatorEntity[FinderYouCoordinator], LightEntity, RestoreEntity):
    """Finder YOU Light entity (Dimmer 15.21 or Relay channel 13.x)."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: FinderYouCoordinator, light: Light) -> None:
        super().__init__(coordinator)
        self._light = light
        self._attr_unique_id = f"{DOMAIN}_light_{light.uuid}_{light.channel}"
        self._attr_name = None

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{light.uuid}_{light.channel}")},
            name=light.name,
            manufacturer="Finder",
            model=light.device_model,
        )

        self._is_on: bool = False
        self._brightness: int = 255 if light.is_dimmer else 0

        if light.is_dimmer:
            self._attr_color_mode = ColorMode.BRIGHTNESS
            self._attr_supported_color_modes = {ColorMode.BRIGHTNESS}
        else:
            self._attr_color_mode = ColorMode.ONOFF
            self._attr_supported_color_modes = {ColorMode.ONOFF}

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        # Seed from live coordinator channel state first if available
        channel_data = self.coordinator.channel_states.get((self._light.uuid, self._light.channel))
        if channel_data is not None and "is_on" in channel_data:
            self._is_on = channel_data["is_on"]
            b = channel_data.get("brightness")
            if self._light.is_dimmer and isinstance(b, int) and b > 0:
                self._brightness = b
            return

        last = await self.async_get_last_state()
        if last is not None:
            self._is_on = (last.state == "on")
            if self._light.is_dimmer:
                b = last.attributes.get(ATTR_BRIGHTNESS)
                if isinstance(b, int) and 0 < b <= 255:
                    self._brightness = b

    @property
    def available(self) -> bool:
        """Return True if entity is available.

        Stays available if local BLE routing is active or if coordinator is healthy,
        preventing lights from becoming unavailable during brief cloud gRPC drops.
        """
        if self.coordinator.enable_local_ble or (self._light.uuid, self._light.channel) in self.coordinator.channel_states:
            return True
        return super().available

    @property
    def is_on(self) -> bool:
        channel_data = self.coordinator.channel_states.get((self._light.uuid, self._light.channel))
        if channel_data is not None and "is_on" in channel_data:
            self._is_on = channel_data["is_on"]
            return self._is_on
        return self._is_on

    @property
    def brightness(self) -> int | None:
        if not self._light.is_dimmer:
            return None
        channel_data = self.coordinator.channel_states.get((self._light.uuid, self._light.channel))
        if channel_data is not None:
            b = channel_data.get("brightness")
            if isinstance(b, int) and b > 0:
                self._brightness = b
        return self._brightness


    @property
    def is_syncing(self) -> bool:
        """Return True if a command is currently syncing or verifying."""
        return self.coordinator.is_channel_syncing(self._light.uuid, self._light.channel)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        return {
            "syncing": self.is_syncing,
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the light, optionally adjusting brightness."""
        if self._light.is_dimmer and ATTR_BRIGHTNESS in kwargs:
            raw_val = kwargs[ATTR_BRIGHTNESS]
            self._brightness = raw_val
            self._is_on = True
            self.coordinator.channel_states.setdefault((self._light.uuid, self._light.channel), {})["is_on"] = True
            self.coordinator.channel_states[(self._light.uuid, self._light.channel)]["brightness"] = raw_val
            self.async_write_ha_state()
            percent = max(1, min(100, round((raw_val / 255) * 100)))
            await self.coordinator.async_set_brightness(
                self._light.uuid, percent, self._light.channel
            )
            return

        self._is_on = True
        self.async_write_ha_state()
        await self.coordinator.async_switch_on(
            self._light.uuid, self._light.channel
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the light."""
        self._is_on = False
        self.async_write_ha_state()
        await self.coordinator.async_switch_off(
            self._light.uuid, self._light.channel
        )
