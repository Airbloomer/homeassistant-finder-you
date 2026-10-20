"""Update coordinator: keeps a long-lived h2 connection alive and polls the
plant for shutter state at a fixed interval. Commands flow through the same
connection so they share the gateway claim established by the boot
sequence."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    BleDirectDevice,
    FinderApiError,
    FinderHomeClient,
    GatewayOfflineError,
    Light,
    OAuthError,
    Scenario,
    Shutter,
    Switch,
    extract_ble_inventory,
    extract_channel_states,
    extract_shutter_durations,
    extract_shutter_motion,
    extract_shutter_positions,
    extract_shutter_states,
    fetch_token,
    parse_lights_and_switches,
    parse_plant,
    parse_scenarios,
    refresh_token,
)

from .const import (
    CONF_ENABLE_LOCAL_BLE,
    DEFAULT_ENABLE_LOCAL_BLE,
    DEFAULT_SCAN_INTERVAL_SECONDS,
    DOMAIN,
)
from .topology import FinderTopologyRouter


# Minimum gap between consecutive command sends. 250 ms was enough to stop
# the WiFi-side burst, but during 6-shutter scenes the gateway's BLE-mesh
# fan-out to the actual shutters still dropped the farthest hops (Cucina,
# Salotto). 2 s gives BLE-mesh time to land each command before the next
# one fires. Single user-initiated taps still feel instant because there's
# nothing queued ahead of them.
COMMAND_SEND_GAP = 2.0
# Fast send gap for light/switch RPCs so multi-light commands don't queue behind 2.0s shutter gaps
LIGHT_COMMAND_SEND_GAP = 0.15
# How long after a command to wait for the gateway's plant-state cache to
# reflect the change. Empirically the cache lags 30-60 s for solo taps but
# can stretch past 90 s during scene bursts. 180 s outwaits the worst case
# we've observed; the alternative (declaring failure when the shutter
# actually moved) is worse than making the user wait.
VERIFY_TIMEOUT = 180.0
# How many times to retry a command if verify fails. Each retry tears
# down the cloud client and re-runs the OpenNotificationChannel handshake,
# which resolves any cloud-side subscription staleness. Three attempts
# total gives the gateway multiple shots at landing the BLE-mesh hop —
# the dominant remaining failure for the shutter that's farthest from
# the puck. Total worst-case time per shutter: 3 × VERIFY_TIMEOUT plus
# a few seconds of reconnect overhead.
MAX_SEND_ATTEMPTS = 3
# How often to re-fetch the plant during verification. Cheap call; tight
# polling makes successful commands feel as fast as possible.
VERIFY_POLL_INTERVAL = 2.0
# If the gateway hasn't pushed any telemetry for this many seconds when
# a user command arrives, force a fresh OpenNotificationChannel handshake
# before the first send attempt. The reactive self-heal already does
# this — but only after a 180 s verify timeout. Doing it preemptively
# when staleness is plausible turns "first scene of the morning waits
# 3 minutes" into "first scene completes in seconds." Threshold picked
# to be well below any normal idle window the cloud-side claim can
# survive (we've seen healthy claims survive >>10 min idle in logs) so
# we don't burn handshakes on innocent quiet stretches.
PREEMPTIVE_HANDSHAKE_TELEMETRY_AGE = 600.0  # 10 min
# How many consecutive fast-path "success" sends (baseline unknown,
# telemetry fresh, no motor evidence ever observed) we tolerate before
# concluding another app has taken the live-client slot. The Finder
# YOU app silently demotes us when it's opened: subsequent SetOpenPercent
# RPCs are accepted by the cloud but never reach the puck. Our fast-path
# can't tell the difference between "shutter already at target" and
# "command went into a void" — both look identical. Threshold of 1
# means we reclaim immediately on the *current* user command rather
# than making the first post-takeover tap silently fail and recovering
# on the second.
PREEMPTIVE_HANDSHAKE_UNVERIFIED_SENDS = 1

# Grace window during which cloud polling data will NOT overwrite local optimistic/BLE state
CHANNEL_STATE_LOCK_WINDOW = 15.0  # seconds
SHUTTER_STATE_LOCK_WINDOW = 60.0  # seconds (fallback lock when BLE confirmed)

# Timeout in seconds to wait for physical BLE telemetry before triggering Cloud Fallback
BLE_STATE_CONFIRMATION_TIMEOUT = 4.5  # seconds (also covers ~4.25s mesh-to-cloud propagation)
BLE_DIMMER_CONFIRMATION_TIMEOUT = 4.5  # seconds (dimmers have a 1.0-3.5s fade ramp)
BLE_GROUP_STAGGER_TIMEOUT = 1.2  # max seconds to hold _ble_switch_lock waiting for ACK before next group light
BLE_POST_ACK_QUIET_GAP = 0.20  # quiet air gap after ACK so mesh repeaters (0x4x) finish before next proxy TX
BLE_MIN_INTER_TX_GAP = 0.35  # minimum gap between consecutive local BLE transmissions
BLE_SHUTTER_ACK_TIMEOUT = 3.5  # seconds (shutters emit initial motor-start BLE packet in ~1.0s)
DEFAULT_SHUTTER_FULL_TRAVEL_SECONDS = 25.0  # fallback if relay Ch1T1/Ch1T2 not yet loaded
SHUTTER_TRAVEL_GUARD_OFFSET = 5.0  # fixed +5s safety margin added to (T_max * delta)

_LOGGER = logging.getLogger(__name__)

# Refresh the JWT a few minutes before it actually expires.
TOKEN_REFRESH_MARGIN = 300  # seconds


class FinderYouCoordinator(DataUpdateCoordinator[dict]):
    """Owns the FinderHomeClient lifecycle and produces shutter state for HA."""

    def __init__(self, hass: HomeAssistant, email: str, password: str) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}/{email}",
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL_SECONDS),
        )
        self._email = email
        self._password = password
        self._client: FinderHomeClient | None = None
        self._client_lock = asyncio.Lock()
        # Serializes the SEND step across all command paths. Verification
        # polls can still overlap because we now diff each shutter's own
        # state slice rather than the whole payload.
        self._send_lock = asyncio.Lock()
        self._last_send_ts: float = 0.0
        self._token: dict | None = None
        self._token_expiry: float = 0
        self._plant_id: bytes | None = None
        self._plant_name: str = ""
        self._shutters: list[Shutter] = []
        self._scenarios: list[Scenario] = []
        self._lights: list[Light] = []
        self._switches: list[Switch] = []
        self._channel_states: dict[tuple[str, int], dict[str, Any]] = {}
        # Wedge characterization: track when the gateway last produced
        # fresh telemetry for *any* shutter, so we can tell whether the
        # cloud-to-puck link is alive without needing a user command.
        # ``_previous_slices`` holds the per-shutter state slice from
        # the last successful poll; any byte-level diff (motion, RSSI,
        # position, anything) counts as the gateway being alive.
        self._previous_slices: dict[str, bytes] = {}
        self._last_telemetry_change_ts: float | None = None
        self._last_successful_command_ts: float | None = None
        # Set whenever ``_ensure_client`` completes a fresh handshake.
        # The watchdog uses this to schedule preemptive rehandshakes.
        self._last_handshake_ts: float | None = None
        # Consecutive fast-path successes (baseline unknown, telemetry
        # fresh) with no motor evidence ever observed. Increments on
        # each fast-path, resets on any real verify that sees motor
        # activity. When it hits PREEMPTIVE_HANDSHAKE_UNVERIFIED_SENDS
        # we assume another app stole the live-client slot and force a
        # rehandshake to reclaim it.
        self._unverified_send_count: int = 0

        # Local-First BLE Direct Mesh Routing with state verification
        self._enable_local_ble: bool = True
        self._ble_inventory: dict[str, BleDirectDevice] = {}
        self._ble_switch_lock = asyncio.Lock()
        self._last_ble_tx_ts: float = 0.0
        self._syncing_channels: set[tuple[str, int]] = set()
        self._channel_lock_until: dict[tuple[str, int], float] = {}
        self._state_change_events: dict[tuple[str, int], asyncio.Event] = {}
        self._pending_ble_target: dict[tuple[str, int], bool] = {}
        self._pending_ble_dimmer_target: dict[tuple[str, int], int] = {}
        self._ble_confirm_guard_until: dict[tuple[str, int], float] = {}
        self._channel_cmd_seq: dict[tuple[str, int], int] = {}
        self._shutter_durations: dict[str, tuple[float, float]] = {}
        self._shutter_lock_until: dict[str, float] = {}
        self._shutter_guard_until: dict[str, float] = {}
        self._pending_ble_shutter_target: dict[str, int] = {}
        self._shutter_start_pos: dict[str, int | None] = {}
        self._shutter_cmd_start_ts: dict[str, float] = {}
        self._shutter_ble_confirmed: dict[str, bool] = {}
        self._shutter_guard_timers: dict[str, asyncio.TimerHandle] = {}
        self._shutter_ack_events: dict[str, asyncio.Event] = {}
        self._shutter_cmd_seq: dict[str, int] = {}
        self._shutter_active_tasks: dict[str, asyncio.Task] = {}
        self._consecutive_update_failures: int = 0
        self._topology_router = FinderTopologyRouter(hass, coordinator=self)
        self._topology_router.start()

    def is_channel_syncing(self, device_uuid: str, channel: int) -> bool:
        """Return True if a channel command is currently being executed or verified."""
        return (device_uuid, channel) in self._syncing_channels

    @property
    def enable_local_ble(self) -> bool:
        return self._enable_local_ble

    @property
    def topology_router(self) -> FinderTopologyRouter:
        return self._topology_router

    @property
    def ble_inventory(self) -> dict[str, BleDirectDevice]:
        return self._ble_inventory


    @property
    def plant_id(self) -> bytes | None:
        return self._plant_id

    @property
    def plant_name(self) -> str:
        return self._plant_name

    @property
    def shutters(self) -> list[Shutter]:
        return self._shutters

    @property
    def scenarios(self) -> list[Scenario]:
        return self._scenarios

    @property
    def lights(self) -> list[Light]:
        return self._lights

    @property
    def switches(self) -> list[Switch]:
        return self._switches

    @property
    def channel_states(self) -> dict[tuple[str, int], dict[str, Any]]:
        return self._channel_states


    @property
    def last_telemetry_change_ts(self) -> float | None:
        """Unix timestamp of the last poll that observed any slice diff."""
        return self._last_telemetry_change_ts

    @property
    def last_successful_command_ts(self) -> float | None:
        """Unix timestamp of the last verified command."""
        return self._last_successful_command_ts

    @property
    def last_handshake_ts(self) -> float | None:
        """Unix timestamp of the last successful OpenNotificationChannel handshake."""
        return self._last_handshake_ts

    async def _ensure_token(self) -> None:
        now = time.time()
        if self._token and now < self._token_expiry - TOKEN_REFRESH_MARGIN:
            return
        if self._token and self._token.get("refresh_token"):
            try:
                self._token = await refresh_token(self._token["refresh_token"])
            except OAuthError:
                _LOGGER.info("refresh failed, doing fresh login")
                self._token = None
        if not self._token:
            self._token = await fetch_token(self._email, self._password)
        self._token_expiry = time.time() + int(self._token.get("expires_in", 3600))

    async def _ensure_client(self) -> FinderHomeClient:
        if self._client is not None:
            return self._client
        await self._ensure_token()
        assert self._token is not None
        try:
            client = await FinderHomeClient.connect(self._token["access_token"])
            plants_msg = await client.handshake()
            # First GetUserPlants response carries the plant UUID in field 1
            # of the inner Plant message (field 3 of the wrapper).
            if 3 in plants_msg:
                from .api.proto import parse_fields  # local import to avoid cycle

                inner = parse_fields(plants_msg[3][0])
                self._plant_id = inner.get(1, [b""])[0]
        except Exception:
            # If anything during handshake fails, drop the client so we'll
            # reconnect on the next update.
            try:
                await client.close()  # type: ignore[name-defined]
            except Exception:
                pass
            raise
        self._client = client
        self._last_handshake_ts = time.time()
        return client

    async def _drop_client(self) -> None:
        """Close + forget the current client so the next call reconnects."""
        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None

    async def _run_or_reconnect(self, fn):
        """Run ``fn(client)`` once. If the connection looks dead (any error or
        timeout), drop the client and retry once with a fresh one.

        If repeated failures occur, also purge the cached OAuth token to force
        a full fresh authentication sequence.
        """
        try:
            client = await self._ensure_client()
            return await asyncio.wait_for(fn(client), timeout=15)
        except Exception as err:
            _LOGGER.info("client call failed (%s); reconnecting", err)
            await self._drop_client()
            if self._consecutive_update_failures >= 2:
                _LOGGER.info("Multiple failures detected; purging token to force fresh login")
                self._token = None
            client = await self._ensure_client()
            return await asyncio.wait_for(fn(client), timeout=15)

    async def _async_update_data(self) -> dict[str, int | None]:
        """Fetch plant + return ``{shutter_uuid: position}`` for HA cover."""
        try:
            plant_payload = await self._run_or_reconnect(
                lambda c: c.get_plant(self._plant_id) if self._plant_id else c.handshake()
            )
            self._consecutive_update_failures = 0
        except (TimeoutError, FinderApiError, ConnectionError, OAuthError, OSError) as err:
            self._consecutive_update_failures += 1
            await self._drop_client()
            if self._consecutive_update_failures >= 3:
                self._token = None
            
            # If we have existing data or local BLE enabled, fall back to cached state
            # for up to 5 consecutive failures (~75s) to avoid UI entities flickering unavailable.
            if self.data and self._consecutive_update_failures <= 5:
                _LOGGER.warning(
                    "Cloud poll failed (%s) [%d/5]; falling back to cached telemetry",
                    err,
                    self._consecutive_update_failures,
                )
                return self.data
            raise UpdateFailed(str(err)) from err

        # If we fell back to handshake (no plant_id yet), the payload is the
        # plants_msg dict, not the raw plant bytes.
        if isinstance(plant_payload, dict):
            return {s.uuid: None for s in self._shutters}

        plant_name, shutters = parse_plant(plant_payload)
        self._plant_name = plant_name or self._plant_name
        if shutters:
            self._shutters = shutters
        scenarios = parse_scenarios(plant_payload)
        if scenarios:
            self._scenarios = scenarios
        lights, switches = parse_lights_and_switches(plant_payload)
        if lights:
            self._lights = lights
        if switches:
            self._switches = switches
        # Populate BLE direct inventory (MAC, family, Master AES-128 key)
        ble_inv = extract_ble_inventory(plant_payload)
        if ble_inv:
            self._ble_inventory = ble_inv
        shutter_durations = extract_shutter_durations(plant_payload)
        if shutter_durations:
            self._shutter_durations.update(shutter_durations)

        cloud_states = extract_channel_states(plant_payload)
        now = time.time()
        # Merge cloud states while respecting local lock window and active BLE telemetry
        for key, st in cloud_states.items():
            if now < self._channel_lock_until.get(key, 0):
                # Channel is locked due to recent local/BLE command; keep current optimistic state
                continue
            ble_dev = self._ble_inventory.get(key[0])
            if (
                ble_dev is not None
                and key in self._channel_states
                and self._topology_router.has_direct_telemetry(ble_dev, max_age=60.0)
            ):
                # Live local BLE telemetry is actively tracking this device in real time;
                # do not let lagging Cloud cache overwrite real-time physical BLE state.
                continue
            self._channel_states[key] = st
        positions = extract_shutter_positions(plant_payload)
        merged_positions: dict[str, int | None] = {}
        existing_data = self.data or {}
        for s in self._shutters:
            u = s.uuid
            curr_pos = existing_data.get(u)
            cloud_pos = positions.get(u)
            if curr_pos is not None:
                if now < self._shutter_lock_until.get(u, 0):
                    merged_positions[u] = curr_pos
                    continue
                ble_dev = self._ble_inventory.get(u)
                if (
                    ble_dev is not None
                    and self._shutter_ble_confirmed.get(u, True)
                    and self._topology_router.has_direct_telemetry(ble_dev, max_age=120.0)
                ):
                    merged_positions[u] = curr_pos
                    continue
            merged_positions[u] = cloud_pos if cloud_pos is not None else curr_pos

        self._track_telemetry_freshness(plant_payload)
        return merged_positions



    def _track_telemetry_freshness(self, plant_payload: bytes) -> None:
        """Update freshness markers and log slice transitions.

        We diff each shutter's per-shutter state slice against the prior
        poll. Any byte-level difference (position, motion, RSSI, etc.)
        is proof the gateway is still pushing data to the cloud — i.e.
        the puck-to-Finder pipe is alive. Stretches of zero diffs while
        commands are being issued, or stretches of any length on a quiet
        plant, are what we want to characterize so we can find the
        threshold that precedes a wedge.

        Logs at INFO level so the user can `journalctl | grep
        finder_you.coordinator` and see the timeline without flipping
        DEBUG on the noisy lower layers.
        """
        new_slices = extract_shutter_states(plant_payload)
        first_poll = not self._previous_slices
        changed: list[str] = []
        for uuid, slc in new_slices.items():
            if self._previous_slices.get(uuid) != slc:
                changed.append(uuid)
        self._previous_slices = new_slices
        # Skip the freshness stamp + log on the very first poll — every
        # shutter looks "changed" because we had no baseline, and that
        # would falsely register as gateway activity at startup.
        if not changed or first_poll:
            return
        self._last_telemetry_change_ts = time.time()
        _LOGGER.info(
            "telemetry: %d shutter(s) updated: %s",
            len(changed),
            ", ".join(u[:8] for u in changed),
        )

    def _telemetry_recent(self) -> bool:
        """Has the gateway pushed any per-shutter slice diff in the
        ``PREEMPTIVE_HANDSHAKE_TELEMETRY_AGE`` window? When true, the
        puck-to-cloud pipe is demonstrably alive even if a particular
        shutter's cached position has expired — which is what lets us
        treat a `baseline_position is None` send as a safe no-op
        without falling for a wedge."""
        if self._last_telemetry_change_ts is None:
            return False
        return time.time() - self._last_telemetry_change_ts < PREEMPTIVE_HANDSHAKE_TELEMETRY_AGE

    def _should_preemptive_rehandshake(self) -> bool:
        """Decide whether to drop the cloud client before the first send.

        Two independent triggers:

          * **Stale telemetry**: the puck hasn't produced any per-shutter
            slice diff for longer than
            ``PREEMPTIVE_HANDSHAKE_TELEMETRY_AGE``. Strong empirical
            signal of cloud-subscription drift (morning of 2026-06-18:
            8 h of zero telemetry before the first scene sat through
            three 180 s verify timeouts).

          * **Live-client takeover**: ``_unverified_send_count`` has
            accumulated past ``PREEMPTIVE_HANDSHAKE_UNVERIFIED_SENDS``.
            When the Finder YOU mobile app is opened it silently grabs
            the live-client slot from us — our SetOpenPercent RPCs are
            accepted by the cloud but never reach the puck. We can't
            distinguish a true no-op (shutter at target) from a silent
            takeover within one command, but consecutive no-evidence
            commands across different shutters/targets are diagnostic.

        Skipped while another command is already in flight (``_send_lock``
        held) — ripping out the client underneath it would just cascade.
        """
        if self._send_lock.locked():
            return False
        if (
            self._last_telemetry_change_ts is not None
            and time.time() - self._last_telemetry_change_ts >= PREEMPTIVE_HANDSHAKE_TELEMETRY_AGE
        ):
            return True
        if self._unverified_send_count >= PREEMPTIVE_HANDSHAKE_UNVERIFIED_SENDS:
            return True
        return False

    async def _send_command(self, shutter_uuid: str, target: int, do_call, seq: int | None = None) -> None:
        """Send a command, retrying with fresh handshake on verify failure.

        Each verify timeout means one of:
          * The gateway's cloud-side subscription has gone stale (server
            restart, claim expiry) and is dropping our RPC silently.
            ``_drop_client`` fixes this on the next call.
          * The gateway's BLE-mesh hop to this specific shutter dropped
            on the floor (congestion, transient link). A fresh re-send
            gets a new shot.

        Both paths benefit from "drop, reconnect, send again". We try up
        to ``MAX_SEND_ATTEMPTS`` times; if all of them fail to elicit
        motor evidence the gateway is truly wedged for this shutter and
        we raise so HA can surface the failure.

        Before the first attempt we may also do a *preemptive*
        rehandshake — see ``_should_preemptive_rehandshake`` — which
        skips an otherwise-guaranteed 180 s verify timeout when the
        gateway's telemetry has been silent long enough to suggest
        subscription drift.
        """
        if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
            return
        if self._should_preemptive_rehandshake():
            tel = (
                f"telemetry {time.time() - self._last_telemetry_change_ts:.0f}s old"
                if self._last_telemetry_change_ts is not None
                else "no telemetry baseline"
            )
            _LOGGER.info(
                "preemptive rehandshake before send (%s, %d unverified sends)",
                tel,
                self._unverified_send_count,
            )
            await self._drop_client()
            # Give the freshly reclaimed claim a clean slate before we
            # start re-incrementing the suspicion counter.
            self._unverified_send_count = 0
        # We allow one in-line reclaim+retry per command: if the first
        # attempt returns via the unknown-baseline fast-path (no motor
        # evidence ever observed) we can't tell whether it was a true
        # no-op or a silent failure due to live-client takeover. Rather
        # than guess, drop the client to force a fresh handshake and
        # try once more. A second fast-path success after a fresh claim
        # is accepted as a true no-op — if even the reclaim didn't
        # surface motor evidence, the shutter is almost certainly at
        # target. Bounded to one reclaim-retry to keep scene UX from
        # paying a handshake cost per shutter when nothing's broken.
        inline_reclaim_used = False
        for attempt in range(MAX_SEND_ATTEMPTS):
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                _LOGGER.info("Aborting superseded Cloud fallback for %s (seq=%d)", shutter_uuid, seq)
                return
            pre_count = self._unverified_send_count
            try:
                await self._send_and_verify(shutter_uuid, target, do_call, seq=seq)
            except GatewayOfflineError:
                if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                    return
                if attempt == MAX_SEND_ATTEMPTS - 1:
                    raise
                _LOGGER.info(
                    "gateway didn't reflect %s on attempt %d/%d; "
                    "dropping client and retrying",
                    shutter_uuid,
                    attempt + 1,
                    MAX_SEND_ATTEMPTS,
                )
                await self._drop_client()
                continue
            if (
                self._unverified_send_count > pre_count
                and not inline_reclaim_used
                and attempt + 1 < MAX_SEND_ATTEMPTS
            ):
                if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                    return
                _LOGGER.info(
                    "%s succeeded only via fast-path; reclaiming live-client "
                    "and retrying once",
                    shutter_uuid,
                )
                await self._drop_client()
                # Roll the suspicion counter back so the retry starts
                # fresh — if it also goes via fast-path we'll re-increment.
                self._unverified_send_count = pre_count
                inline_reclaim_used = True
                continue
            return

    async def _send_and_verify(
        self, shutter_uuid: str, target: int, do_call, seq: int | None = None
    ) -> None:
        """One send-then-verify cycle.

        The YESLY gateway's WiFi/MQTT link silently drops commands when
        more than one or two arrive within ~100 ms, and its BLE-mesh
        fan-out to the actual shutters drops the farthest hops when six
        shutters fire concurrently. Defense:

          * Serialize sends through ``_send_lock`` with ``COMMAND_SEND_GAP``
            between consecutive calls so neither the WiFi link nor the
            BLE-mesh sees a burst.
          * After the cloud accepts the send, capture the shutter's
            current position and motion flag and poll the plant until we
            see *evidence the motor actually ran* (see
            ``_wait_for_motor_evidence``) or ``VERIFY_TIMEOUT`` elapses.

        Why two signals (position + motion) instead of just position:
        the position field changes when the shutter reports its new
        physical reading back to the gateway via BLE-mesh telemetry —
        usually within seconds, but the cache lag can stretch past 90 s
        during scene bursts, and on a shutter whose pre-send position
        was already empty (Camera Alex in our setup) we have no baseline
        to detect a change against. The motion flag (#12) reads 3 while
        the gateway is actively driving the motor, so observing it even
        briefly is positive proof that the BLE-mesh hop reached the
        shutter — independent of whether telemetry has come back yet.

        Short-circuit: if the shutter's position already matches
        ``target`` we don't expect any motor movement, so we send the
        command for safety but skip the verify wait.

        On verify timeout we raise ``GatewayOfflineError`` so the caller
        can drop+retry or surface the failure to HA.
        """
        async with self._send_lock:
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return
            loop = asyncio.get_event_loop()
            wait = COMMAND_SEND_GAP - (loop.time() - self._last_send_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return
            baseline_payload = await self._run_or_reconnect(lambda c: c.get_plant(self._plant_id))
            await self._run_or_reconnect(do_call)
            self._last_send_ts = asyncio.get_event_loop().time()

        if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
            return
        baseline_position = extract_shutter_positions(baseline_payload).get(shutter_uuid)
        if baseline_position == target:
            # Already at target — no motor movement to detect. This is
            # a definite no-op (we saw the exact target value), so we
            # also reset the suspicion counter: the cloud showed us a
            # current value, which means the puck-to-cloud pipe is
            # alive on our subscription.
            self._unverified_send_count = 0
            self._last_successful_command_ts = time.time()
            await self.async_request_refresh()
            return
        if baseline_position is None:
            # Shutters without telemetry in plant cache (or whose telemetry
            # has expired) don't report position or baseline.
            # Check briefly for motor movement (motion flag 3 or position appearance).
            # If not detected within a short window, accept optimistically.
            if await self._wait_for_motor_evidence(shutter_uuid, baseline_position, timeout=6.0, seq=seq):
                self._unverified_send_count = 0
                _LOGGER.debug("%s baseline unknown but motor movement observed", shutter_uuid)
            else:
                self._unverified_send_count += 1
                _LOGGER.debug(
                    "%s baseline unknown and no motor movement within 6s — treating as accepted/no-op (suspicion %d)",
                    shutter_uuid,
                    self._unverified_send_count,
                )
            self._last_successful_command_ts = time.time()
            await self.async_request_refresh()
            return
        if not await self._wait_for_motor_evidence(shutter_uuid, baseline_position, seq=seq):
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return
            raise GatewayOfflineError(
                f"gateway didn't reflect {shutter_uuid} within "
                f"{VERIFY_TIMEOUT:.0f}s — likely offline or congested"
            )
        # Real motor evidence — the puck heard us and drove the motor.
        # That's positive proof we still hold the live-client claim.
        self._unverified_send_count = 0
        # Verify succeeded: kick a coordinator refresh so HA's state
        # picks up the new position the next time HomeKit reads it.
        self._last_successful_command_ts = time.time()
        await self.async_request_refresh()

    async def _wait_for_motor_evidence(
        self,
        shutter_uuid: str,
        baseline_position: int | None,
        timeout: float = VERIFY_TIMEOUT,
        seq: int | None = None,
    ) -> bool:
        """Poll the plant for proof the shutter motor actually ran.

        Either signal counts as evidence:
          * Position field changes from ``baseline_position`` — the
            shutter has reported a new physical reading back to the
            gateway. Most reliable, but needs a known baseline and is
            subject to telemetry cache lag.
          * Motion flag (#12) observed at 3 ("moving") — the gateway is
            actively driving the motor over BLE-mesh. Faster than
            position telemetry and doesn't need a baseline; the only way
            we'd observe 3 is if the BLE-mesh hop reached the shutter.
        """
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return True
            if self._shutter_ble_confirmed.get(shutter_uuid, False):
                return True
            await asyncio.sleep(VERIFY_POLL_INTERVAL)
            if seq is not None and self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return True
            try:
                payload = await self._run_or_reconnect(lambda c: c.get_plant(self._plant_id))
            except Exception:
                _LOGGER.debug("verify poll failed", exc_info=True)
                continue
            if extract_shutter_positions(payload).get(shutter_uuid) != baseline_position:
                return True
            if extract_shutter_motion(payload).get(shutter_uuid) == 3:
                return True
        return False

    def _compute_shutter_travel_guard(
        self, shutter_uuid: str, current_pos: int | None, target_pos: int
    ) -> tuple[float, float, float]:
        """Compute ``(guard_seconds, expected_travel_seconds, full_travel_seconds)`` using relay Ch1T1/Ch1T2."""
        open_s, close_s = self._shutter_durations.get(
            shutter_uuid,
            (DEFAULT_SHUTTER_FULL_TRAVEL_SECONDS, DEFAULT_SHUTTER_FULL_TRAVEL_SECONDS),
        )
        if current_pos is not None:
            delta = abs(target_pos - current_pos) / 100.0
            full_s = open_s if target_pos >= current_pos else close_s
            expected_travel = full_s * delta
        else:
            full_s = max(open_s, close_s)
            expected_travel = full_s
        guard_s = expected_travel + SHUTTER_TRAVEL_GUARD_OFFSET
        return guard_s, expected_travel, full_s

    def _estimate_shutter_position_now(self, shutter_uuid: str, now: float) -> int | None:
        """Estimate the shutter's physical position when a command is interrupted mid-guard."""
        if (
            now < self._shutter_guard_until.get(shutter_uuid, 0)
            and self._shutter_start_pos.get(shutter_uuid) is not None
        ):
            prev_start = self._shutter_start_pos.get(shutter_uuid)
            prev_target = self._pending_ble_shutter_target.get(shutter_uuid)
            cmd_start = self._shutter_cmd_start_ts.get(shutter_uuid, now)
            if prev_start is not None and prev_target is not None:
                _, prev_travel, _ = self._compute_shutter_travel_guard(
                    shutter_uuid, prev_start, prev_target
                )
                elapsed = max(0.0, now - cmd_start - 0.5)
                if prev_travel <= 0.1 or elapsed >= prev_travel:
                    return prev_target
                ratio = min(1.0, elapsed / prev_travel)
                return round(prev_start + (prev_target - prev_start) * ratio)
            return prev_start
        return self.data.get(shutter_uuid) if self.data is not None else None

    def _cancel_shutter_active_task(self, shutter_uuid: str) -> None:
        """Cancel any in-flight command or cloud fallback task for shutter_uuid."""
        prev_task = self._shutter_active_tasks.pop(shutter_uuid, None)
        curr_task = asyncio.current_task()
        if prev_task is not None and prev_task is not curr_task and not prev_task.done():
            prev_task.cancel()

    def _cancel_shutter_guard_timer(self, shutter_uuid: str) -> None:
        """Cancel any active post-travel guard timer for shutter_uuid."""
        handle = self._shutter_guard_timers.pop(shutter_uuid, None)
        if handle is not None:
            handle.cancel()

    def _schedule_shutter_guard_check(self, shutter_uuid: str, seq: int, guard_s: float) -> None:
        """Schedule a timely feedback check right when the dynamic travel guard expires."""
        self._cancel_shutter_guard_timer(shutter_uuid)
        self._shutter_guard_timers[shutter_uuid] = self.hass.loop.call_later(
            guard_s, self._on_shutter_guard_expired, shutter_uuid, seq
        )

    @callback
    def _on_shutter_guard_expired(self, shutter_uuid: str, seq: int) -> None:
        """Release travel guard when (T_max * delta + 5s) expires and request feedback refresh if needed."""
        self._shutter_guard_timers.pop(shutter_uuid, None)
        if self._shutter_cmd_seq.get(shutter_uuid) != seq:
            return
        self._pending_ble_shutter_target.pop(shutter_uuid, None)
        self._shutter_guard_until.pop(shutter_uuid, None)
        if not self._shutter_ble_confirmed.get(shutter_uuid, False):
            self._shutter_lock_until.pop(shutter_uuid, None)
            _LOGGER.info(
                "Dynamic travel guard expired for %s without final BLE stop packet; triggering timely feedback refresh",
                shutter_uuid,
            )
            self.hass.async_create_task(self.async_request_refresh())

    async def async_set_position(self, shutter_uuid: str, percent: int) -> None:
        """Set shutter position (0-100%) with Local-First BLE routing, ack verification, and cloud fallback."""
        seq = self._shutter_cmd_seq.get(shutter_uuid, 0) + 1
        self._shutter_cmd_seq[shutter_uuid] = seq
        self._cancel_shutter_active_task(shutter_uuid)
        curr_task = asyncio.current_task()
        if curr_task is not None:
            self._shutter_active_tasks[shutter_uuid] = curr_task

        prev_evt = self._shutter_ack_events.get(shutter_uuid)
        if prev_evt and not prev_evt.is_set():
            prev_evt.set()

        now = time.time()
        current_pos = self._estimate_shutter_position_now(shutter_uuid, now)
        guard_s, expected_travel, full_s = self._compute_shutter_travel_guard(
            shutter_uuid, current_pos, percent
        )
        self._shutter_start_pos[shutter_uuid] = current_pos
        self._shutter_cmd_start_ts[shutter_uuid] = now
        self._shutter_ble_confirmed[shutter_uuid] = False
        self._shutter_lock_until[shutter_uuid] = now + guard_s + SHUTTER_STATE_LOCK_WINDOW
        self._pending_ble_shutter_target[shutter_uuid] = percent
        self._shutter_guard_until[shutter_uuid] = now + guard_s
        self._schedule_shutter_guard_check(shutter_uuid, seq, guard_s)
        _LOGGER.info(
            "Dynamic shutter travel guard for %s: %.1fs (travel=%.1fs + %.1fs offset, from=%s%% -> to=%d%%, T_max=%.0fs)",
            shutter_uuid,
            guard_s,
            expected_travel,
            SHUTTER_TRAVEL_GUARD_OFFSET,
            current_pos if current_pos is not None else "?",
            percent,
            full_s,
        )

        if self.data is not None:
            self.data[shutter_uuid] = percent
            self.async_set_updated_data(self.data)

        try:
            if self._enable_local_ble and shutter_uuid in self._ble_inventory:
                ble_dev = self._ble_inventory[shutter_uuid]
                primary_node = self._topology_router.select_best_node(ble_dev)
                alt_node = self._topology_router.select_alternate_node(ble_dev, exclude_node=primary_node)
                primary_rssi = self._topology_router.get_node_rssi(ble_dev, primary_node)
                _LOGGER.info(
                    "Attempting Local BLE direct position %d%% for %s (%s) via '%s' (rssi=%s, alt='%s')",
                    percent,
                    shutter_uuid,
                    ble_dev.mac,
                    primary_node,
                    f"{primary_rssi}dBm" if primary_rssi is not None else "?",
                    alt_node,
                )

                ack_evt = asyncio.Event()
                self._shutter_ack_events[shutter_uuid] = ack_evt
                dual_dispatched = False

                try:
                    if await self._topology_router.async_dispatch_set_percent(ble_dev, percent, preferred_node=primary_node):
                        if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                            return
                        # For marginal-signal shutters (< -85 dBm or unknown RSSI), proactively also transmit
                        # the idempotent set_percent on the adjacent proxy after 0.55s if not yet acked
                        if alt_node and (primary_rssi is None or primary_rssi < -85):
                            try:
                                await asyncio.wait_for(ack_evt.wait(), timeout=0.55)
                                if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                    return
                                self._shutter_ble_confirmed[shutter_uuid] = True
                                self._last_successful_command_ts = time.time()
                                _LOGGER.info("Local BLE shutter command %d%% acknowledged by %s", percent, shutter_uuid)
                                return
                            except asyncio.TimeoutError:
                                if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                    return
                                _LOGGER.info(
                                    "Proactive dual-proxy BLE transmit for marginal shutter %s (%s) via '%s'",
                                    shutter_uuid,
                                    ble_dev.mac,
                                    alt_node,
                                )
                                await self._topology_router.async_dispatch_set_percent(
                                    ble_dev, percent, preferred_node=alt_node
                                )
                                dual_dispatched = True

                        try:
                            await asyncio.wait_for(ack_evt.wait(), timeout=BLE_SHUTTER_ACK_TIMEOUT)
                            if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                return
                            self._shutter_ble_confirmed[shutter_uuid] = True
                            self._last_successful_command_ts = time.time()
                            _LOGGER.info("Local BLE shutter command %d%% acknowledged by %s", percent, shutter_uuid)
                            return
                        except asyncio.TimeoutError:
                            if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                return
                            if alt_node and not dual_dispatched:
                                _LOGGER.warning(
                                    "No BLE motion ack from shutter %s within %.1fs via '%s'; retrying via alternate proxy '%s'",
                                    shutter_uuid,
                                    BLE_SHUTTER_ACK_TIMEOUT,
                                    primary_node,
                                    alt_node,
                                )
                                ack_evt.clear()
                                if await self._topology_router.async_dispatch_set_percent(ble_dev, percent, preferred_node=alt_node):
                                    if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                        return
                                    try:
                                        await asyncio.wait_for(ack_evt.wait(), timeout=BLE_SHUTTER_ACK_TIMEOUT)
                                        if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                            return
                                        self._shutter_ble_confirmed[shutter_uuid] = True
                                        self._last_successful_command_ts = time.time()
                                        _LOGGER.info(
                                            "Local BLE shutter command %d%% acknowledged by %s via alternate proxy '%s'",
                                            percent,
                                            shutter_uuid,
                                            alt_node,
                                        )
                                        return
                                    except asyncio.TimeoutError:
                                        if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                                            return
                            _LOGGER.warning(
                                "Local BLE shutter position %d%% unconfirmed by %s via all proxies; falling back to Finder Cloud",
                                percent,
                                shutter_uuid,
                            )
                    else:
                        _LOGGER.warning("Local BLE set_position dispatch failed for %s, falling back to Finder Cloud", shutter_uuid)
                finally:
                    if self._shutter_ack_events.get(shutter_uuid) is ack_evt:
                        self._shutter_ack_events.pop(shutter_uuid, None)

            if self._shutter_cmd_seq.get(shutter_uuid) != seq:
                return

            now_fb = time.time()
            self._shutter_cmd_start_ts[shutter_uuid] = now_fb
            self._shutter_guard_until[shutter_uuid] = now_fb + guard_s
            self._shutter_lock_until[shutter_uuid] = now_fb + guard_s + SHUTTER_STATE_LOCK_WINDOW
            self._schedule_shutter_guard_check(shutter_uuid, seq, guard_s)

            assert self._plant_id is not None
            await self._send_command(
                shutter_uuid,
                percent,
                lambda c: c.set_open_percent(self._plant_id, shutter_uuid.encode(), percent),
                seq=seq,
            )
        except asyncio.CancelledError:
            _LOGGER.info("Cancelled superseded shutter command seq=%d for %s", seq, shutter_uuid)
            return
        finally:
            if self._shutter_active_tasks.get(shutter_uuid) is curr_task:
                self._shutter_active_tasks.pop(shutter_uuid, None)

    async def async_open(self, shutter_uuid: str) -> None:
        """Open shutter full (100%) using idempotent set_position."""
        await self.async_set_position(shutter_uuid, 100)

    async def async_close_shutter(self, shutter_uuid: str) -> None:
        """Close shutter full (0%) using idempotent set_position."""
        await self.async_set_position(shutter_uuid, 0)

    async def async_stop_shutter(self, shutter_uuid: str) -> None:
        """Stop shutter movement immediately and release travel guard so actual stopped position updates."""
        now = time.time()
        est_pos = self._estimate_shutter_position_now(shutter_uuid, now)
        seq = self._shutter_cmd_seq.get(shutter_uuid, 0) + 1
        self._shutter_cmd_seq[shutter_uuid] = seq
        self._cancel_shutter_active_task(shutter_uuid)
        self._cancel_shutter_guard_timer(shutter_uuid)
        self._pending_ble_shutter_target.pop(shutter_uuid, None)
        self._shutter_guard_until.pop(shutter_uuid, None)
        self._shutter_lock_until.pop(shutter_uuid, None)
        prev_evt = self._shutter_ack_events.get(shutter_uuid)
        if prev_evt and not prev_evt.is_set():
            prev_evt.set()

        if self.data is not None and est_pos is not None:
            self.data[shutter_uuid] = est_pos
            self.async_set_updated_data(self.data)

        if self._enable_local_ble and shutter_uuid in self._ble_inventory:
            ble_dev = self._ble_inventory[shutter_uuid]
            primary_node = self._topology_router.select_best_node(ble_dev)
            alt_node = self._topology_router.select_alternate_node(ble_dev, exclude_node=primary_node)
            primary_rssi = self._topology_router.get_node_rssi(ble_dev, primary_node)
            _LOGGER.info("Attempting Local BLE shutter stop for %s (%s) via '%s'", shutter_uuid, ble_dev.mac, primary_node)
            dispatched = await self._topology_router.async_dispatch_shutter_stop(
                ble_dev, 1, preferred_node=primary_node
            )
            if alt_node and (not dispatched or primary_rssi is None or primary_rssi < -85):
                await asyncio.sleep(0.45)
                await self._topology_router.async_dispatch_shutter_stop(
                    ble_dev, 1, preferred_node=alt_node
                )
                dispatched = True
            if dispatched:
                self._last_successful_command_ts = time.time()
                return


    async def async_activate_scenario(self, scenario_uuid: str) -> None:
        """Activate a cloud scenario."""
        assert self._plant_id is not None
        await self._run_or_reconnect(
            lambda c: c.activate_scenario(self._plant_id, scenario_uuid.encode())
        )
        _LOGGER.info("activated scenario %s", scenario_uuid)

    def _is_dimmer_device(self, device_uuid: str) -> bool:
        """Return True if device_uuid is a dimmer light."""
        return any(l.uuid == device_uuid and l.is_dimmer for l in self._lights)

    async def _fetch_cloud_channel_state(
        self, device_uuid: str, channel: int
    ) -> dict[str, Any] | None:
        """Fetch fresh plant state from Finder Cloud and return channel state dict for (device_uuid, channel)."""
        key = (device_uuid, channel)
        try:
            plant_payload = await self._run_or_reconnect(
                lambda c: c.get_plant(self._plant_id) if self._plant_id else c.handshake()
            )
            if not isinstance(plant_payload, bytes):
                return None
            cloud_states = extract_channel_states(plant_payload)
            now_check = time.time()
            for k_other, st_other in cloud_states.items():
                if k_other == key:
                    continue
                if now_check < self._channel_lock_until.get(k_other, 0):
                    continue
                b_other = self._ble_inventory.get(k_other[0])
                if (
                    b_other is not None
                    and k_other in self._channel_states
                    and self._topology_router.has_direct_telemetry(b_other, max_age=60.0)
                ):
                    continue
                self._channel_states[k_other] = st_other
            return cloud_states.get(key)
        except Exception as err:
            _LOGGER.warning(
                "Failed to re-read Cloud state for %s Ch %d: %s",
                device_uuid,
                channel,
                err,
            )
            return None

    async def _execute_cloud_switch_with_state_check(
        self,
        device_uuid: str,
        channel: int,
        target_is_on: bool,
        seq: int | None = None,
        ble_dispatched: bool = False,
    ) -> None:
        """Re-read current channel state from Finder Cloud and only send Cloud switch command if needed."""
        key = (device_uuid, channel)
        action_name = "switch_on" if target_is_on else "switch_off"
        target_label = "ON" if target_is_on else "OFF"
        ble_dev = self._ble_inventory.get(device_uuid)

        if seq is not None and self._channel_cmd_seq.get(key) != seq:
            return

        assert self._plant_id is not None
        if self._should_preemptive_rehandshake():
            _LOGGER.info("preemptive rehandshake before %s", action_name)
            await self._drop_client()

        async with self._send_lock:
            if seq is not None and self._channel_cmd_seq.get(key) != seq:
                return

            if (
                ble_dev is not None
                and self._channel_states.get(key, {}).get("is_on") is target_is_on
                and self._topology_router.has_direct_telemetry(ble_dev, max_age=5.0)
            ):
                _LOGGER.info(
                    "Late live BLE telemetry confirmed %s Ch %d is already %s; skipping Cloud %s",
                    device_uuid,
                    channel,
                    target_label,
                    action_name,
                )
                return

            max_checks = 2 if ble_dispatched else 1
            cloud_is_on: bool | None = None
            ch_info: dict[str, Any] | None = None

            for check_idx in range(max_checks):
                if seq is not None and self._channel_cmd_seq.get(key) != seq:
                    return
                ch_info = await self._fetch_cloud_channel_state(device_uuid, channel)
                if ch_info is not None and "is_on" in ch_info:
                    cloud_is_on = ch_info["is_on"]

                if (
                    ble_dev is not None
                    and self._channel_states.get(key, {}).get("is_on") is target_is_on
                    and self._topology_router.has_direct_telemetry(ble_dev, max_age=5.0)
                ):
                    _LOGGER.info(
                        "Live BLE telemetry confirmed %s Ch %d -> %s during Cloud state check; skipping Cloud %s",
                        device_uuid,
                        channel,
                        target_label,
                        action_name,
                    )
                    return

                if cloud_is_on is target_is_on:
                    self._channel_states.setdefault(key, {})["is_on"] = target_is_on
                    if ch_info and "brightness" in ch_info and ch_info["brightness"]:
                        self._channel_states[key]["brightness"] = ch_info["brightness"]
                    self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
                    self._last_successful_command_ts = time.time()
                    _LOGGER.info(
                        "Cloud state re-read confirms %s Ch %d is already %s in Cloud; aligning state and skipping Cloud %s",
                        device_uuid,
                        channel,
                        target_label,
                        action_name,
                    )
                    self.async_set_updated_data(self.data)
                    return

                if check_idx + 1 < max_checks:
                    _LOGGER.info(
                        "Cloud state for %s Ch %d is still %s after local BLE click (check %d/%d); waiting 1.5s for mesh-to-cloud propagation",
                        device_uuid,
                        channel,
                        cloud_is_on,
                        check_idx + 1,
                        max_checks,
                    )
                    await asyncio.sleep(1.5)

            if cloud_is_on is not None and cloud_is_on != target_is_on:
                _LOGGER.info(
                    "Cloud state re-read for %s Ch %d reports is_on=%s (target=%s); aligning pre-send state and sending Cloud %s",
                    device_uuid,
                    channel,
                    cloud_is_on,
                    target_is_on,
                    action_name,
                )
                self._channel_states.setdefault(key, {})["is_on"] = cloud_is_on

            if seq is not None and self._channel_cmd_seq.get(key) != seq:
                return

            loop = asyncio.get_event_loop()
            wait = LIGHT_COMMAND_SEND_GAP - (loop.time() - self._last_send_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            if target_is_on:
                await self._run_or_reconnect(
                    lambda c: c.switch_on(self._plant_id, device_uuid.encode(), channel)
                )
            else:
                await self._run_or_reconnect(
                    lambda c: c.switch_off(self._plant_id, device_uuid.encode(), channel)
                )
            self._last_send_ts = asyncio.get_event_loop().time()

        self._channel_states.setdefault(key, {})["is_on"] = target_is_on
        self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
        self._last_successful_command_ts = time.time()
        _LOGGER.info(
            "switched %s device %s channel %d via Cloud (after Cloud state verification)",
            "on" if target_is_on else "off",
            device_uuid,
            channel,
        )
        self.async_set_updated_data(self.data)

    async def _async_ble_switch_channel(
        self,
        ble_dev: BleDirectDevice,
        channel: int,
        target_is_on: bool,
        seq: int,
        is_dimmer: bool,
    ) -> tuple[bool, bool]:
        """Serialize local BLE click across groups/scenes and wait for physical BLE telemetry confirmation.

        Returns (ble_confirmed, ble_dispatched).
        """
        device_uuid = ble_dev.uuid
        key = (device_uuid, channel)
        action_name = "switch_on" if target_is_on else "switch_off"
        target_label = "ON" if target_is_on else "OFF"
        confirm_timeout = BLE_DIMMER_CONFIRMATION_TIMEOUT if is_dimmer else BLE_STATE_CONFIRMATION_TIMEOUT

        change_evt = asyncio.Event()
        self._pending_ble_target[key] = target_is_on
        self._state_change_events[key] = change_evt
        dispatched = False
        remaining_timeout = 0.0

        try:
            async with self._ble_switch_lock:
                if self._channel_cmd_seq.get(key) != seq:
                    return True, False

                if (
                    self._channel_states.get(key, {}).get("is_on") is target_is_on
                    and self._topology_router.has_direct_telemetry(ble_dev, max_age=10.0)
                ):
                    _LOGGER.info(
                        "Device %s Ch %d already confirmed %s via live BLE while queued; skipping BLE click",
                        device_uuid,
                        channel,
                        target_label,
                    )
                    return True, False

                gap_wait = BLE_MIN_INTER_TX_GAP - (time.time() - self._last_ble_tx_ts)
                if gap_wait > 0:
                    await asyncio.sleep(gap_wait)
                    if self._channel_cmd_seq.get(key) != seq:
                        return True, False

                _LOGGER.info(
                    "Attempting Local BLE direct %s for %s Ch %d (%s)",
                    action_name,
                    device_uuid,
                    channel,
                    ble_dev.mac,
                )
                dispatched = await self._topology_router.async_dispatch_click(ble_dev, channel)
                self._last_ble_tx_ts = time.time()

                if not dispatched:
                    _LOGGER.warning("Local BLE dispatch returned False for %s Ch %d", device_uuid, channel)
                    return False, False

                if self._channel_cmd_seq.get(key) != seq:
                    return True, True

                # Optimistic update and start confirmation window from actual TX timestamp
                tx_done_ts = time.time()
                self._channel_states.setdefault(key, {})["is_on"] = target_is_on
                self._ble_confirm_guard_until[key] = tx_done_ts + confirm_timeout
                self._channel_lock_until[key] = tx_done_ts + CHANNEL_STATE_LOCK_WINDOW
                self.async_set_updated_data(self.data)

                stagger_wait = min(confirm_timeout, BLE_GROUP_STAGGER_TIMEOUT)
                try:
                    await asyncio.wait_for(change_evt.wait(), timeout=stagger_wait)
                    if self._channel_cmd_seq.get(key) != seq:
                        return True, True
                    self._ble_confirm_guard_until[key] = time.time() + 3.0
                    self._last_successful_command_ts = time.time()
                    _LOGGER.info("Local BLE %s verified physically for %s Ch %d", action_name, device_uuid, channel)
                    await asyncio.sleep(BLE_POST_ACK_QUIET_GAP)
                    return True, True
                except asyncio.TimeoutError:
                    elapsed = time.time() - tx_done_ts
                    remaining_timeout = max(0.1, confirm_timeout - elapsed)

            # Wait remaining confirmation window outside _ble_switch_lock so other group lights can proceed
            if self._channel_cmd_seq.get(key) != seq:
                return True, True

            try:
                await asyncio.wait_for(change_evt.wait(), timeout=remaining_timeout)
                if self._channel_cmd_seq.get(key) != seq:
                    return True, True
                self._ble_confirm_guard_until[key] = time.time() + 3.0
                self._last_successful_command_ts = time.time()
                _LOGGER.info("Local BLE %s verified physically for %s Ch %d", action_name, device_uuid, channel)
                return True, True
            except asyncio.TimeoutError:
                if self._channel_cmd_seq.get(key) != seq:
                    return True, True
                if is_dimmer:
                    _LOGGER.warning(
                        "Local BLE dimmer %s for %s Ch %d did not receive BLE confirm within %.1fs; skipping Cloud toggle to prevent state inversion",
                        action_name,
                        device_uuid,
                        channel,
                        confirm_timeout,
                    )
                    return True, True
                self._ble_confirm_guard_until.pop(key, None)
                _LOGGER.warning(
                    "Local BLE %s did not receive state confirmation within %.1fs for %s Ch %d; checking Cloud state before Fallback",
                    action_name,
                    confirm_timeout,
                    device_uuid,
                    channel,
                )
                return False, True
        finally:
            if self._state_change_events.get(key) is change_evt:
                self._state_change_events.pop(key, None)
                self._pending_ble_target.pop(key, None)

    async def async_switch_on(self, device_uuid: str, channel: int = 1) -> None:
        """Switch on a light or plug channel with Local-First BLE routing and cloud fallback."""
        key = (device_uuid, channel)
        now = time.time()

        # 1. Deduplicate identical in-flight BLE command on the same channel
        if self._pending_ble_target.get(key) is True and key in self._state_change_events:
            _LOGGER.info("Ignoring duplicate switch_on for %s Ch %d: identical BLE command already in flight", device_uuid, channel)
            return

        curr_is_on = self._channel_states.get(key, {}).get("is_on")

        # 2. Deduplicate redundant command within post-confirmation guard window
        if curr_is_on is True and now < self._ble_confirm_guard_until.get(key, 0):
            _LOGGER.info("Skipping redundant switch_on for %s Ch %d: ON state was just confirmed", device_uuid, channel)
            return

        seq = self._channel_cmd_seq.get(key, 0) + 1
        self._channel_cmd_seq[key] = seq
        self._pending_ble_dimmer_target.pop(key, None)
        prev_evt = self._state_change_events.get(key)
        if prev_evt and not prev_evt.is_set():
            prev_evt.set()

        self._channel_lock_until[key] = now + CHANNEL_STATE_LOCK_WINDOW
        is_dimmer = self._is_dimmer_device(device_uuid)
        ble_dispatched = False

        if self._enable_local_ble and device_uuid in self._ble_inventory:
            ble_dev = self._ble_inventory[device_uuid]

            if curr_is_on is True:
                # Device is already ON according to local coordinator state.
                if self._topology_router.has_direct_telemetry(ble_dev, max_age=60.0):
                    _LOGGER.info(
                        "Device %s Ch %d is already confirmed ON via live BLE; skipping redundant switch_on",
                        device_uuid,
                        channel,
                    )
                    return
                # Re-read Cloud state first to verify if it is genuinely ON or misaligned
                _LOGGER.info(
                    "Device %s Ch %d is marked ON locally without recent BLE telemetry; re-reading Cloud state before switch_on",
                    device_uuid,
                    channel,
                )
                ch_info = await self._fetch_cloud_channel_state(device_uuid, channel)
                if self._channel_cmd_seq.get(key) != seq:
                    return
                cloud_is_on = ch_info.get("is_on") if ch_info else None
                if cloud_is_on is True:
                    self._channel_states.setdefault(key, {})["is_on"] = True
                    self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
                    self._last_successful_command_ts = time.time()
                    _LOGGER.info(
                        "Cloud state re-read confirms %s Ch %d is already ON; skipping switch_on",
                        device_uuid,
                        channel,
                    )
                    self.async_set_updated_data(self.data)
                    return
                if cloud_is_on is False:
                    _LOGGER.info(
                        "Cloud state re-read reveals %s Ch %d is actually OFF (local state was misaligned); proceeding with Local BLE switch_on",
                        device_uuid,
                        channel,
                    )
                    self._channel_states.setdefault(key, {})["is_on"] = False
                    curr_is_on = False

            if curr_is_on is not True:
                ble_confirmed, ble_dispatched = await self._async_ble_switch_channel(
                    ble_dev, channel, target_is_on=True, seq=seq, is_dimmer=is_dimmer
                )
                if ble_confirmed:
                    return

        await self._execute_cloud_switch_with_state_check(
            device_uuid, channel, target_is_on=True, seq=seq, ble_dispatched=ble_dispatched
        )

    async def async_switch_off(self, device_uuid: str, channel: int = 1) -> None:
        """Switch off a light or plug channel with Local-First BLE routing and cloud fallback."""
        key = (device_uuid, channel)
        now = time.time()

        # 1. Deduplicate identical in-flight BLE command on the same channel
        if self._pending_ble_target.get(key) is False and key in self._state_change_events:
            _LOGGER.info("Ignoring duplicate switch_off for %s Ch %d: identical BLE command already in flight", device_uuid, channel)
            return

        curr_is_on = self._channel_states.get(key, {}).get("is_on")

        # 2. Deduplicate redundant command within post-confirmation guard window
        if curr_is_on is False and now < self._ble_confirm_guard_until.get(key, 0):
            _LOGGER.info("Skipping redundant switch_off for %s Ch %d: OFF state was just confirmed", device_uuid, channel)
            return

        seq = self._channel_cmd_seq.get(key, 0) + 1
        self._channel_cmd_seq[key] = seq
        self._pending_ble_dimmer_target.pop(key, None)
        prev_evt = self._state_change_events.get(key)
        if prev_evt and not prev_evt.is_set():
            prev_evt.set()

        self._channel_lock_until[key] = now + CHANNEL_STATE_LOCK_WINDOW
        is_dimmer = self._is_dimmer_device(device_uuid)
        ble_dispatched = False

        if self._enable_local_ble and device_uuid in self._ble_inventory:
            ble_dev = self._ble_inventory[device_uuid]

            if curr_is_on is False:
                # Device is already OFF according to local coordinator state.
                if self._topology_router.has_direct_telemetry(ble_dev, max_age=60.0):
                    _LOGGER.info(
                        "Device %s Ch %d is already confirmed OFF via live BLE; skipping redundant switch_off",
                        device_uuid,
                        channel,
                    )
                    return
                # Re-read Cloud state first to verify if it is genuinely OFF or misaligned
                _LOGGER.info(
                    "Device %s Ch %d is marked OFF locally without recent BLE telemetry; re-reading Cloud state before switch_off",
                    device_uuid,
                    channel,
                )
                ch_info = await self._fetch_cloud_channel_state(device_uuid, channel)
                if self._channel_cmd_seq.get(key) != seq:
                    return
                cloud_is_on = ch_info.get("is_on") if ch_info else None
                if cloud_is_on is False:
                    self._channel_states.setdefault(key, {})["is_on"] = False
                    self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
                    self._last_successful_command_ts = time.time()
                    _LOGGER.info(
                        "Cloud state re-read confirms %s Ch %d is already OFF; skipping switch_off",
                        device_uuid,
                        channel,
                    )
                    self.async_set_updated_data(self.data)
                    return
                if cloud_is_on is True:
                    _LOGGER.info(
                        "Cloud state re-read reveals %s Ch %d is actually ON (local state was misaligned); proceeding with Local BLE switch_off",
                        device_uuid,
                        channel,
                    )
                    self._channel_states.setdefault(key, {})["is_on"] = True
                    curr_is_on = True

            if curr_is_on is not False:
                ble_confirmed, ble_dispatched = await self._async_ble_switch_channel(
                    ble_dev, channel, target_is_on=False, seq=seq, is_dimmer=is_dimmer
                )
                if ble_confirmed:
                    return

        await self._execute_cloud_switch_with_state_check(
            device_uuid, channel, target_is_on=False, seq=seq, ble_dispatched=ble_dispatched
        )

    async def async_set_brightness(self, device_uuid: str, percent: int, channel: int = 1) -> None:
        """Set brightness level (0-100%) on a dimmer channel with Local-First BLE routing and cloud fallback."""
        key = (device_uuid, channel)
        seq = self._channel_cmd_seq.get(key, 0) + 1
        self._channel_cmd_seq[key] = seq
        self._pending_ble_target.pop(key, None)
        prev_evt = self._state_change_events.get(key)
        if prev_evt and not prev_evt.is_set():
            prev_evt.set()

        now = time.time()
        self._channel_lock_until[key] = now + CHANNEL_STATE_LOCK_WINDOW
        self._pending_ble_dimmer_target[key] = percent
        self._ble_confirm_guard_until[key] = now + BLE_DIMMER_CONFIRMATION_TIMEOUT

        ch_state = self._channel_states.setdefault(key, {})
        ch_state["is_on"] = (percent > 0)
        if percent > 0:
            ch_state["brightness"] = round((percent / 100) * 255)

        if self._enable_local_ble and device_uuid in self._ble_inventory:
            ble_dev = self._ble_inventory[device_uuid]
            async with self._ble_switch_lock:
                if self._channel_cmd_seq.get(key) != seq:
                    return
                gap_wait = BLE_MIN_INTER_TX_GAP - (time.time() - self._last_ble_tx_ts)
                if gap_wait > 0:
                    await asyncio.sleep(gap_wait)
                _LOGGER.info("Attempting Local BLE direct brightness %d%% for %s (%s)", percent, device_uuid, ble_dev.mac)
                dispatched = await self._topology_router.async_dispatch_set_percent(ble_dev, percent)
                self._last_ble_tx_ts = time.time()
            if dispatched:
                if self._channel_cmd_seq.get(key) != seq:
                    return
                self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
                self._ble_confirm_guard_until[key] = time.time() + BLE_DIMMER_CONFIRMATION_TIMEOUT
                self._last_successful_command_ts = time.time()
                _LOGGER.info("Local BLE set_brightness succeeded for %s to %d%%", device_uuid, percent)
                self.async_set_updated_data(self.data)
                return
            _LOGGER.warning("Local BLE set_brightness failed for %s, falling back to Finder Cloud", device_uuid)

        if self._channel_cmd_seq.get(key) != seq:
            return

        assert self._plant_id is not None
        async with self._send_lock:
            if self._channel_cmd_seq.get(key) != seq:
                return
            ch_info = await self._fetch_cloud_channel_state(device_uuid, channel)
            if self._channel_cmd_seq.get(key) != seq:
                return
            if ch_info is not None:
                cloud_is_on = ch_info.get("is_on")
                cloud_br = ch_info.get("brightness", 0)
                cloud_pct = round((cloud_br / 255) * 100) if isinstance(cloud_br, int) else 0
                if (percent == 0 and cloud_is_on is False) or (
                    percent > 0 and cloud_is_on is True and abs(cloud_pct - percent) <= 3
                ):
                    _LOGGER.info(
                        "Cloud state re-read confirms dimmer %s Ch %d is already at %d%%; skipping Cloud set_brightness",
                        device_uuid,
                        channel,
                        percent,
                    )
                    self.async_set_updated_data(self.data)
                    return
            loop = asyncio.get_event_loop()
            wait = LIGHT_COMMAND_SEND_GAP - (loop.time() - self._last_send_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            await self._run_or_reconnect(
                lambda c: c.set_brightness(self._plant_id, device_uuid.encode(), percent, channel)
            )
            self._last_send_ts = asyncio.get_event_loop().time()
        ch_state = self._channel_states.setdefault(key, {})
        ch_state["is_on"] = (percent > 0)
        if percent > 0:
            ch_state["brightness"] = round((percent / 100) * 255)
        self._channel_lock_until[key] = time.time() + CHANNEL_STATE_LOCK_WINDOW
        self._ble_confirm_guard_until[key] = time.time() + BLE_DIMMER_CONFIRMATION_TIMEOUT
        self._last_successful_command_ts = time.time()
        _LOGGER.info("set brightness on device %s channel %d to %d%% via Cloud", device_uuid, channel, percent)
        self.async_set_updated_data(self.data)


    async def _send_cloud_switch(self, device_uuid: str, channel: int, target_is_on: bool) -> None:
        """Helper to send cloud switch on/off command after verifying current Cloud state."""
        await self._execute_cloud_switch_with_state_check(
            device_uuid, channel, target_is_on=target_is_on, seq=self._channel_cmd_seq.get((device_uuid, channel))
        )

    async def _async_verify_switch_state(self, device_uuid: str, channel: int, target_is_on: bool) -> None:
        """Polls cloud state after local BLE toggle. If out-of-sync after grace window, triggers cloud fallback."""
        key = (device_uuid, channel)
        try:
            # Poll up to 6 seconds (mesh telemetry to gateway takes ~3-4.5s)
            deadline = asyncio.get_event_loop().time() + 6.0
            matched = False
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(1.5)
                plant_payload = await self._run_or_reconnect(
                    lambda c: c.get_plant(self._plant_id) if self._plant_id else c.handshake()
                )
                if isinstance(plant_payload, bytes):
                    new_states = extract_channel_states(plant_payload)
                    real_state = new_states.get(key, {}).get("is_on")
                    if real_state == target_is_on:
                        matched = True
                        _LOGGER.debug("BLE switch verified via mesh telemetry for %s Ch %d (%s)", device_uuid, channel, real_state)
                        break

            if not matched:
                _LOGGER.warning(
                    "State mismatch detected for %s Ch %d (expected %s). Triggering active Cloud Fallback...",
                    device_uuid, channel, target_is_on
                )
                await self._send_cloud_switch(device_uuid, channel, target_is_on)
        except Exception as err:
            _LOGGER.debug("Failed background state verification for %s Ch %d: %s", device_uuid, channel, err)
        finally:
            self._syncing_channels.discard(key)
            self.async_set_updated_data(self.data)

    @callback
    def handle_ble_device_state_update(
        self,
        device_uuid: str,
        channel_states: dict[int, bool] | None = None,
        percent: int | None = None,
    ) -> None:
        """Immediately update entity states upon sniffing physical switch / shutter BLE packet."""
        updated = False
        now = time.time()
        if channel_states:
            for ch, is_on in channel_states.items():
                key = (device_uuid, ch)
                pending_target = self._pending_ble_target.get(key)
                if pending_target is not None:
                    if is_on == pending_target:
                        # Physical relay confirmed the commanded target state!
                        self._channel_states.setdefault(key, {})["is_on"] = is_on
                        self._ble_confirm_guard_until[key] = now + 2.0
                        updated = True
                        _LOGGER.info(
                            "Live BLE telemetry confirmed target state: Device %s Channel %d -> %s",
                            device_uuid,
                            ch,
                            "ON" if is_on else "OFF",
                        )
                        evt = self._state_change_events.get(key)
                        if evt and not evt.is_set():
                            evt.set()
                    else:
                        # Pre-switching BLE packet still carrying the old state while command is in flight; ignore
                        _LOGGER.debug(
                            "Ignoring pre-transition BLE state (%s) for %s Ch %d while waiting for %s",
                            "ON" if is_on else "OFF",
                            device_uuid,
                            ch,
                            "ON" if pending_target else "OFF",
                        )
                    continue

                curr = self._channel_states.get(key, {}).get("is_on")
                if curr != is_on:
                    if now < self._ble_confirm_guard_until.get(key, 0):
                        _LOGGER.debug(
                            "Ignoring post-command BLE echo (%s) for %s Ch %d within guard window",
                            "ON" if is_on else "OFF",
                            device_uuid,
                            ch,
                        )
                        continue
                    self._channel_states.setdefault(key, {})["is_on"] = is_on
                    updated = True
                    _LOGGER.info(
                        "Live BLE telemetry update: Device %s Channel %d -> %s",
                        device_uuid,
                        ch,
                        "ON" if is_on else "OFF",
                    )

        if percent is not None:
            is_dimmer_light = self._is_dimmer_device(device_uuid)

            if is_dimmer_light:
                key = (device_uuid, 1)
                is_on = (percent > 0)
                br = round((percent / 100) * 255)
                st = self._channel_states.setdefault(key, {})

                # 1. Check if we are waiting for ON/OFF confirmation after async_switch_on / async_switch_off
                pending_target = self._pending_ble_target.get(key)
                if pending_target is not None:
                    if is_on == pending_target:
                        st["is_on"] = is_on
                        if br > 0:
                            st["brightness"] = br
                        self._ble_confirm_guard_until[key] = now + 2.5
                        updated = True
                        _LOGGER.info(
                            "Live BLE telemetry confirmed target state: Dimmer %s -> %s (%d%%, br=%d)",
                            device_uuid,
                            "ON" if is_on else "OFF",
                            percent,
                            br,
                        )
                        evt = self._state_change_events.get(key)
                        if evt and not evt.is_set():
                            evt.set()
                    else:
                        _LOGGER.debug(
                            "Ignoring pre-transition BLE dimmer state (%s, %d%%) for %s while waiting for %s",
                            "ON" if is_on else "OFF",
                            percent,
                            device_uuid,
                            "ON" if pending_target else "OFF",
                        )
                    if updated:
                        self._last_telemetry_change_ts = now
                        self.async_set_updated_data(self.data)
                    return

                # 2. Check if we are within the post-command ramp/echo guard window
                if now < self._ble_confirm_guard_until.get(key, 0):
                    target_pct = self._pending_ble_dimmer_target.get(key)
                    if target_pct is not None:
                        reached = (
                            abs(percent - target_pct) <= 3
                            or (target_pct >= 90 and percent >= 90)
                            or (target_pct == 0 and percent == 0)
                        )
                        if not reached:
                            _LOGGER.debug(
                                "Ignoring transient ramp BLE dimmer packet (%d%%) for %s while ramping to %d%%",
                                percent,
                                device_uuid,
                                target_pct,
                            )
                            return
                        self._pending_ble_dimmer_target.pop(key, None)
                        self._ble_confirm_guard_until.pop(key, None)
                    elif st.get("is_on") != is_on:
                        _LOGGER.debug(
                            "Ignoring post-command BLE dimmer echo (%s, %d%%) for %s within guard window",
                            "ON" if is_on else "OFF",
                            percent,
                            device_uuid,
                        )
                        return

                prev_on = st.get("is_on")
                prev_br = st.get("brightness")
                new_br = br if br > 0 else prev_br
                if prev_on != is_on or (br > 0 and prev_br != br):
                    st["is_on"] = is_on
                    if new_br is not None:
                        st["brightness"] = new_br
                    updated = True
                    _LOGGER.info(
                        "Live BLE telemetry update: Dimmer %s -> %s (%d%%, br=%d)",
                        device_uuid,
                        "ON" if is_on else "OFF",
                        percent,
                        br,
                    )
            elif self.data is not None:
                # Shutter position update
                ack_evt = self._shutter_ack_events.get(device_uuid)
                if ack_evt and not ack_evt.is_set():
                    ack_evt.set()

                if now < self._shutter_guard_until.get(device_uuid, 0):
                    target_pct = self._pending_ble_shutter_target.get(device_uuid)
                    if target_pct is not None:
                        start_pos = self._shutter_start_pos.get(device_uuid)
                        cmd_start_ts = self._shutter_cmd_start_ts.get(device_uuid, 0.0)
                        target_delta = abs(target_pct - (start_pos if start_pos is not None else target_pct))
                        arrival_tolerance = 1 if target_delta <= 5 else 3

                        if abs(percent - target_pct) <= arrival_tolerance:
                            self._cancel_shutter_guard_timer(device_uuid)
                            self._pending_ble_shutter_target.pop(device_uuid, None)
                            self._shutter_guard_until.pop(device_uuid, None)
                            self._shutter_ble_confirmed[device_uuid] = True
                            self._shutter_lock_until[device_uuid] = now + SHUTTER_STATE_LOCK_WINDOW
                            self.data[device_uuid] = percent
                            updated = True
                            _LOGGER.info(
                                "Live BLE telemetry confirmed target position: Shutter %s -> %d%% (target %d%%)",
                                device_uuid,
                                percent,
                                target_pct,
                            )
                        elif start_pos is not None and abs(percent - start_pos) <= 1:
                            _LOGGER.info(
                                "Shutter %s BLE motion telemetry at %d%% (moving from %d%% toward target %d%%; holding UI slider)",
                                device_uuid,
                                percent,
                                start_pos,
                                target_pct,
                            )
                            return
                        elif (
                            start_pos is not None
                            and (now - cmd_start_ts) >= 2.2
                            and abs(percent - start_pos) >= 2
                        ):
                            self._cancel_shutter_guard_timer(device_uuid)
                            self._pending_ble_shutter_target.pop(device_uuid, None)
                            self._shutter_guard_until.pop(device_uuid, None)
                            self._shutter_ble_confirmed[device_uuid] = True
                            self._shutter_lock_until[device_uuid] = now + SHUTTER_STATE_LOCK_WINDOW
                            self.data[device_uuid] = percent
                            updated = True
                            _LOGGER.info(
                                "Live BLE telemetry intermediate stop position: Shutter %s -> %d%% (commanded %d%%)",
                                device_uuid,
                                percent,
                                target_pct,
                            )
                        else:
                            _LOGGER.info(
                                "Shutter %s BLE motion telemetry at %d%% (moving toward target %d%%; holding UI slider)",
                                device_uuid,
                                percent,
                                target_pct,
                            )
                            return

                old_pos = self.data.get(device_uuid)
                if old_pos != percent:
                    self.data[device_uuid] = percent
                    self._shutter_ble_confirmed[device_uuid] = True
                    self._shutter_lock_until[device_uuid] = now + SHUTTER_STATE_LOCK_WINDOW
                    updated = True
                    _LOGGER.info(
                        "Live BLE telemetry update: Shutter %s Position -> %d%%",
                        device_uuid,
                        percent,
                    )

        if updated:
            self._last_telemetry_change_ts = time.time()
            self.async_set_updated_data(self.data)

    async def async_shutdown(self) -> None:
        for task in self._shutter_active_tasks.values():
            if not task.done():
                task.cancel()
        self._shutter_active_tasks.clear()
        for handle in self._shutter_guard_timers.values():
            handle.cancel()
        self._shutter_guard_timers.clear()
        self._topology_router.stop()
        if self._client:
            await self._client.close()
            self._client = None


