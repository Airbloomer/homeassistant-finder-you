"""Scene entity for each Finder YOU cloud scenario."""

from __future__ import annotations

from typing import Any

from homeassistant.components.scene import Scene
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import Scenario
from .const import DOMAIN
from .coordinator import FinderYouCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: FinderYouCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        FinderYouScene(coordinator, scenario) for scenario in coordinator.scenarios
    )


class FinderYouScene(CoordinatorEntity[FinderYouCoordinator], Scene):
    """A cloud macro/scenario exposed as an HA scene."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: FinderYouCoordinator, scenario: Scenario) -> None:
        super().__init__(coordinator)
        self._scenario = scenario
        self._attr_unique_id = f"{DOMAIN}_scenario_{scenario.uuid}"
        self._attr_name = scenario.name
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"scenario_{scenario.uuid}")},
            name=scenario.name,
            manufacturer="Finder",
            model="YESLY Cloud Scenario",
        )

    async def async_activate(self, **kwargs: Any) -> None:
        """Activate this scenario on the Finder YOU cloud."""
        await self.coordinator.async_activate_scenario(self._scenario.uuid)
