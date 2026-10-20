"""Finder YOU Python API client.

A reverse-engineered async client for the ``you-api.iot.findernet.com``
gRPC service. Mints tokens via the Android-style OAuth flow and maintains
a persistent HTTP/2 connection with Android-exact framing so the gateway
accepts our control commands.
"""

from .client import FinderApiError, FinderHomeClient, GatewayOfflineError
from .oauth import OAuthError, fetch_token, refresh_token
from .plant import (
    BleDirectDevice,
    Light,
    Scenario,
    Shutter,
    Switch,
    extract_ble_inventory,
    extract_channel_states,
    extract_shutter_durations,
    extract_shutter_motion,
    extract_shutter_positions,
    extract_shutter_states,
    parse_lights_and_switches,
    parse_plant,
    parse_scenarios,
)

__all__ = [
    "BleDirectDevice",
    "FinderApiError",
    "FinderHomeClient",
    "GatewayOfflineError",
    "Light",
    "OAuthError",
    "Scenario",
    "Shutter",
    "Switch",
    "extract_ble_inventory",
    "extract_channel_states",
    "extract_shutter_durations",
    "extract_shutter_motion",
    "extract_shutter_positions",
    "extract_shutter_states",
    "fetch_token",
    "parse_lights_and_switches",
    "parse_plant",
    "parse_scenarios",
    "refresh_token",
]


