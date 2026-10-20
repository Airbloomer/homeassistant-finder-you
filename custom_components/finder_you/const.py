"""Constants for the Finder YOU integration."""

from __future__ import annotations

DOMAIN = "finder_you"

CONF_EMAIL = "email"
CONF_PASSWORD = "password"
CONF_PLANT_ID = "plant_id"
CONF_ENABLE_LOCAL_BLE = "enable_local_ble"

DEFAULT_SCAN_INTERVAL_SECONDS = 30
DEFAULT_ENABLE_LOCAL_BLE = True

PLATFORMS: list[str] = ["cover", "sensor", "scene", "light", "switch"]
