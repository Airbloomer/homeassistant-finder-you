# Baseline code review — HA Finder YOU

**Repo:** [Airbloomer/homeassistant-finder-you](https://github.com/Airbloomer/homeassistant-finder-you) (public)  
**Reviewed ref:** `main` @ `f9abadef86987b031793c500e1a0ccf1272fd65a`  
**Date:** 2026-09-14  
**Reviewer:** first-ever Airbloomer org review  
**Scope:** secrets exposure (public repo), correctness, auth, tests  
**This document is findings only.** It does not change product code and is not a merge candidate.

---

## Executive summary

This is a Home Assistant custom integration that impersonates the official Finder YOU Android client: it mints OAuth tokens from the user's email + password, holds a long-lived raw HTTP/2 connection to `you-api.iot.findernet.com`, and drives YESLY roller shutters via reverse-engineered gRPC. The protocol work and the command-verify / live-client-reclaim logic are unusually well documented in `PROTOCOL.md` and have a large pytest suite (186 tests, 95.20% line coverage on the latest `main` CI run).

**No live credentials, tokens, private keys, or production JWTs were found in the tree or in git history.** That is the most important public-repo result. Remaining risk is *code that can leak secrets at runtime* and *missing hygiene* (no secret scanning, thin `.gitignore`, OAuth error strings that embed token JSON).

The integration is not yet at an org-ready quality bar:

1. **Auth is password-centric and incomplete.** The config entry stores the Finder password in plaintext HA storage, never persists the refresh token, has no reauth flow, and never raises `ConfigEntryAuthFailed`. Failed token responses can be logged in full.
2. **Correctness of the cloud session is racy.** `_client_lock` is allocated and never used. Concurrent poll / verify / command paths can open two "live clients" against a protocol that only honors one, then hang HA service calls for up to `3 × 180 s`.
3. **Command success can be a lie.** `PROTOCOL.md` itself says a `100 → None` position transition is *not* motor evidence; `_wait_for_motor_evidence` still treats any position change — including cache-clear to `None` — as success. The unknown-baseline fast-path can also report success with no evidence this shutter moved.
4. **CI on `main` is red.** Tests pass; `ruff format --check` has failed on the last six pushes. Coverage is at the 95% gate by construction and does not exercise `extract_shutter_motion` or the diagnostic sensor values.
5. **Public docs point at the wrong GitHub org** (`7AC/homeassistant-finder-you` in README + `manifest.json`), which is an install and trust issue for a public HACS integration.

No Critical findings. Highest-severity items are High (auth + correctness). Do not merge this PR; use it as the backlog for follow-up work.

| Severity | Count | Theme |
|----------|------:|-------|
| Critical | 0 | No live secret leak, no unauthenticated device control |
| High     | 6 | Auth lifecycle, session races, false command success, schema fragility |
| Medium   | 11 | Runtime secret/PII leakage, discovery, CI, public-repo hygiene |
| Low      | 8 | Docs drift, HTTP/2 gaps, coverage theater, pinning |

---

## Method

Reviewed every file under `custom_components/finder_you/`, `tests/`, `.github/`, `PROTOCOL.md`, `README.md`, `hacs.json`, `pyproject.toml`, and `.gitignore`. Scanned all 23 commits on `main` for credential-shaped strings (JWTs, `sk_live`, `ghp_`, PEM private keys, AWS keys, real emails). Pulled the latest GitHub Actions run on `main` (`28407208693`) for coverage and the actual failing job.

Did **not** hit the Finder cloud, replay captured traffic, or load the integration in a live Home Assistant instance.

---

## Secrets exposure (public repo)

### Scan result — clean of live secrets

| Check | Result |
|-------|--------|
| Working tree (`*.py`, `*.md`, `*.json`, `*.yml`) | No live tokens, passwords, or keys |
| `git log -p` / blob search across all commits | Only placeholders (`u@example.com`, `pw`, `<password>`, `Bearer <jwt>`) |
| Test fixtures | Synthetic protobuf + mocked HTTP; no captured plant dumps |
| GitHub Actions logs inspected | No secret values in the failed lint / passing test output |

The Android OAuth `client_id` (`com.findernet.You`) is a **public client identifier**, not a client secret. Publishing it in `oauth.py` / `PROTOCOL.md` matches the official app and is expected.

### What *would* become a Critical if it landed

`PROTOCOL.md` documents that `GetPlant` payloads contain **device UUIDs and AES-128 keys** used for local BLE-mesh signing. The current parser does not extract those keys and tests do not commit raw plant blobs. If anyone later adds a debug log of the raw plant payload, a pytest fixture cut from a real capture, or a GitHub issue attachment, those keys become a public-repo incident. Treat plant payloads as secret material.

---

## Findings

### High

#### H1 — Password is the long-lived credential; no reauth, no `ConfigEntryAuthFailed`

**Where:** `config_flow.py`, `__init__.py`, `coordinator.py` `_ensure_token`

The config flow writes `{email, password}` into the config entry and never stores `access_token` / `refresh_token`. Every HA restart and every refresh-token failure re-posts the user's Finder password to `accounts.iot.findernet.com`. There is no `async_step_reauth`, no options flow, and no `ConfigEntryAuthFailed` — a changed or revoked password surfaces as a generic coordinator `UpdateFailed` / "unknown" form error and the integration keeps retrying the same bad password.

HA stores `core.config_entries` in plaintext under `.storage/`. That is common, but here the password is *the* session secret (tokens live only in RAM). A stolen HA backup is a stolen Finder account.

**Ask:** Persist the refresh token (HA application credentials / `ConfigEntry.data` without the password after first mint, or a dedicated token file), add a reauth flow, and map `OAuthError` on sign-in to `ConfigEntryAuthFailed` so HA stops polling and prompts the user.

#### H2 — OAuth exceptions can embed token JSON and HTTP bodies

**Where:** `api/oauth.py` lines 89, 115–118, 138–141; `config_flow.py` line 36

```python
raise OAuthError(f"token exchange failed: {r.status_code} {r.text[:200]}")
raise OAuthError(f"token response missing access_token: {token}")
# ...
_LOGGER.warning("auth failed: %s", err)
```

A 200 response that includes `refresh_token` / `id_token` but omits `access_token` logs the **entire token dict**. Failed `/connect/token` bodies are also interpolated. Those strings land in HA logs (and anywhere logs are shipped). On a public integration this is the most likely *runtime* secret leak.

**Ask:** Log status codes and sanitized error keys only. Never interpolate `r.text`, `r.json()`, or the token dict.

#### H3 — `_client_lock` is dead; concurrent reconnects fight for the exclusive live-client slot

**Where:** `coordinator.py` line 96 (only reference in the repo)

The Finder cloud grants `OpenNotificationChannel` live-client status to **one** TCP session. `_ensure_client`, `_drop_client`, and `_run_or_reconnect` are called from:

- the 60 s coordinator poll
- verify polls every 2 s **outside** `_send_lock`
- user commands
- preemptive / in-line reclaim (`_drop_client` mid-scene)

Without a lock, two overlapping failures both `connect()` + `handshake()`. The last writer wins `self._client`; the other connection is leaked and may still hold (or have just stolen) the claim. That is the exact failure mode the reclaim logic is trying to paper over.

**Ask:** Guard client create/drop with `_client_lock`. Do not drop the client from a verify poll while a command owns the session.

#### H4 — Verify treats cache-clear (`position → None`) as motor evidence

**Where:** `coordinator.py` `_wait_for_motor_evidence`; contradicted by `PROTOCOL.md` §3

```python
if extract_shutter_positions(payload).get(shutter_uuid) != baseline_position:
    return True
```

`extract_shutter_positions` omits the shutter when field `#13` is empty, so `.get(...)` is `None`. `100 != None` is success. `PROTOCOL.md` states this exact transition happens when the gateway *queues* a command and is **not** proof the motor ran. Motion flag `== 3` is OR'd afterwards; it does not gate the false positive.

Result: HA / HomeKit report a successful move when the shutter never moved.

**Ask:** Count a position change only when both old and new values are concrete ints in `0..100` (and, if you want belt-and-suspenders, require motion `== 3` *or* a numeric position delta). Add a unit test for `baseline=100` then missing `#13`.

#### H5 — Unknown-baseline fast-path reports success with no evidence this shutter moved

**Where:** `coordinator.py` `_send_and_verify` (`baseline_position is None and self._telemetry_recent()`)

If *any* other shutter produced a slice diff in the last 10 minutes, a command against a shutter with no cached position is accepted. The in-line reclaim (threshold 1) mitigates live-client takeover but still accepts a second fast-path as a "true no-op". A scene of already-unknown shutters can therefore succeed while the puck never heard the command.

This is a conscious UX trade-off (avoid 9-minute `Closing…`) documented in `PROTOCOL.md`. It is still incorrect as a success signal.

**Ask:** Surface these as "assumed" / "unknown" rather than verified success, or require motion evidence when baseline is missing.

#### H6 — Per-shutter state extraction uses an exact field-id set

**Where:** `api/plant.py` `_STATE_FIELD_SET` + `set(sub_fields.keys()) != _STATE_FIELD_SET`

If Finder adds or removes one field on the device-state submessage, **every** position, motion, and telemetry-freshness path returns empty. Covers then stick on last-commanded position (default 100 = open). There is no log when the schema filter drops all candidates.

**Ask:** Match on a *required subset* (`{1, 12, 13}` plus UUID shape), log a warning when field 12 entries exist but none match, and keep a captured anonymized plant fixture in tests.

---

### Medium

#### M1 — Command path can block a Home Assistant service call for ~9 minutes

`MAX_SEND_ATTEMPTS = 3` × `VERIFY_TIMEOUT = 180` plus reconnect. Cover `_send` awaits this on the HA event loop service handler. A wedged gateway freezes automations and HomeKit for the full budget before raising `HomeAssistantError`.

**Ask:** Cap the user-visible wait (e.g. 30 s), continue verify in the background, or fail fast after the first timeout and self-heal asynchronously.

#### M2 — Cover entities are a one-shot snapshot

`__init__.py` and `cover.py` both call `async_config_entry_first_refresh`. Entities are added only from `coordinator.shutters` at that moment. Shutters that appear on a later poll (gateway cache lag, new device) never become entities. There is no `async_add_entities` listener.

#### M3 — Diagnostic sensors have global unique IDs and no `DeviceInfo`

`sensor.py`: `unique_id = f"{DOMAIN}_gateway_{suffix}"`. A second config entry collides. Sensors are not attached to a gateway device, so they float in the entity registry. `native_value` is untested (see T2).

#### M4 — OAuth callback parsing can `KeyError`

`fetch_token` does `parse_qs(...)["returnUrl"][0]` and `["code"][0]` with no guard. A 302 that is not the expected shape raises `KeyError`, which the config flow maps to `"unknown"` rather than `"invalid_auth"`. Tests do not cover these branches.

#### M5 — Handshake proceeds without a claim ack

`handshake()` swallows 3 s / 5 s timeouts on all three `OpenNotificationChannel` acks and still starts the 30 s keepalive + returns success. `_ensure_client` then caches a client that may not hold the claim (`code 19` on the next `GetPlant` / `SetOpenPercent`). Combined with H3 this produces reconnect storms.

#### M6 — HTTP/2 notification stream does not reassemble gRPC messages

Unary RPCs buffer DATA until `END_STREAM`. The notification stream treats each DATA frame as one complete 5-byte-prefixed message (`if len(body) >= 5`). Split or coalesced frames drop or corrupt handshake acks and shutter events. `wait_for_shutter_event` is now unused by the coordinator but handshake still depends on this queue.

#### M7 — `main` CI is red (format only); ruff is unpinned

Latest run [28407208693](https://github.com/Airbloomer/homeassistant-finder-you/actions/runs/28407208693): tests, HACS, hassfest **passed**; `ruff format --check` failed on `coordinator.py` and `tests/test_coordinator.py`. The six most recent `main` pushes are red for the same reason. CI installs `ruff` unpinned (`pip install ruff` → 0.15.20 at fail time) and pins `hacs/action@main` + `hassfest@master`.

A public repo with a permanently red badge is a trust problem. Pin ruff; make format part of the same lockfile as tests.

#### M8 — No secret scanning, thin `.gitignore`

`.gitignore` covers pyc / venv / IDE / `.mcp.json` only. Missing: `.env`, `.env.*`, `secrets.yaml`, `*.pem`, `.storage/`, `coverage.xml`, `htmlcov/`, `auth.json`. There is no gitleaks / trufflehog / push protection workflow. For a **public** integration that handles passwords and plant payloads, this is the cheapest High-severity-preventer.

#### M9 — Public install URLs still point at `7AC/`, not `Airbloomer/`

`README.md` HACS instructions, `manifest.json` `documentation` / `issue_tracker`, and `codeowners: ["@7AC"]` advertise a different GitHub org than this public repo. Users who follow the README do not install this tree. License copyright is also `7AC`.

#### M10 — Email is used as coordinator name (PII in logs)

`name=f"{DOMAIN}/{email}"` plus INFO logs for every telemetry diff and reclaim. Fine for a single-user home; noisy and identifying if logs are shared in GitHub issues (the current support path).

#### M11 — Plant-payload AES keys are documented but have no handling policy

See scan section. Add a contrib rule: never commit raw `GetPlant` bytes; redact field blobs in issue templates; do not log payloads above DEBUG, and not at DEBUG by default.

---

### Low

#### L1 — README is stale vs the code

README still says "Position feedback and scenes land in v0.2" and "v0.1.0. Shutters only." Position polling, motor-evidence verify, diagnostic sensors, and scene serialization are already in `main`.

#### L2 — Coverage-for-coverage tests

`tests/test_const.py` exists to "touch every line". Several client tests exist to hit `except` branches. Coverage is 95.20% against a 95% gate — the gate is doing almost no work. `extract_shutter_motion` (the other half of H4) is **0%** covered (`plant.py` 143–151).

#### L3 — `parse_varint` / `parse_fields` have no bounds checks

A truncated or hostile length-delimited field raises `IndexError` (or allocates a huge slice). Plant walkers swallow this; `_call` does not. A bad unary response can take down the in-flight RPC.

#### L4 — `asyncio.get_event_loop()` in client and coordinator

Deprecated; should be `get_running_loop()`. Same for `create_future()`.

#### L5 — No `STOP` cover feature; default last-commanded position is 100

Restored state handles the HomeKit `Closing…` bug on restart; a first-ever install with empty plant cache still reports open.

#### L6 — Token refresh and `_ensure_token` are unlocked

Refresh-token rotation on the IdP can invalidate the other in-flight refresh (H3-adjacent).

#### L7 — `CheckUser` / `PlatformCheck` sent without `Authorization`

Documented as required by the cloud. Not a vulnerability in this repo; do not accidentally add a bearer later without re-testing `code 1` / `code 19`.

#### L8 — `iot_class: cloud_polling` understates the persistent h2 session

Minor hassfest / quality-scale nits. Also no `quality_scale.yaml`, no repo-root `CODEOWNERS`.

---

## Auth notes (beyond H1/H2)

The Android-style no-PKCE flow is a **Finder cloud property**, not a bug the integration introduced. Anyone with the user's password can mint `api.v1` tokens; this client is equivalent to the official Android app. Call that out in the README (unique password, no shared family accounts if possible). `impersonateUsername: None` is sent on every sign-in — keep it `None`; do not expose it in the UI.

Refresh tokens stay in RAM only. That reduces disk leakage and increases password use (H1). Pick one model and implement it fully.

SSL: `fetch_token` uses a cached `ssl.create_default_context()` off the event loop (correct for HA); `refresh_token` uses httpx `verify=True`. Both verify certs. No `verify=False` anywhere.

---

## Tests

| Area | What exists | Gap |
|------|-------------|-----|
| OAuth | Happy path + most HTTP error statuses (respx) | Missing `returnUrl` / `code` KeyError; no assert that error strings omit bodies |
| Config flow | form / invalid / unknown / success / duplicate | No reauth (feature missing) |
| Coordinator | Large, good on send-gap, self-heal, fast-path, preemptive handshake | No concurrent `_ensure_client`; H4 cache-clear case untested |
| Plant | Strong synthetic fixtures for parse + positions | **`extract_shutter_motion` untested**; no real (redacted) capture |
| Client | Framing, dispatcher, `_call`, handshake with fakes | No multi-frame gRPC reassembly test (M6) |
| Cover | Restore, position fallback, `GatewayOfflineError` | No HA-level service timeout test |
| Sensor | Import-level only (69% cover) | `native_value` None vs timestamp untested |
| Init | setup / unload with patched coordinator | Forward-setup patched away — platforms never load together |

**CI snapshot (`main` @ f9abade):** 186 passed, 95.20% total. Fail-under 95% is already "barely green" and will go red the next time anyone adds an untested branch. That is why format-only failures have been ignored — the test job looks healthy.

There are no contract tests against recorded HTTP/2 frames. Given how protocol-sensitive this client is (`code 19`, SETTINGS, three-message subscribe), a committed *redacted* capture would lock the handshake better than more mocked `AsyncMock` tests.

---

## Suggested fix order (not in this PR)

1. Sanitize OAuth errors + add gitleaks + expand `.gitignore` (M8, H2) — public-repo blast radius.
2. Reauth + `ConfigEntryAuthFailed` + stop logging the password path on every retry (H1).
3. Honor `_client_lock`; never drop the client from a verify poll (H3).
4. Fix motor-evidence predicates + tests (H4, H5, L2).
5. Relax state-schema match + log drops (H6).
6. Bound command wait (M1); discover covers dynamically (M2); scope sensor unique IDs (M3).
7. Pin ruff, format `coordinator.py` / `tests/test_coordinator.py`, go green on `main` (M7).
8. Point README / manifest at `Airbloomer/homeassistant-finder-you` (M9). Update README status (L1).

---

## Out of scope / not reviewed

- Live Finder cloud behavior, BLE-mesh timing, or a real YESLY plant
- Home Assistant Core / HomeKit bridge versions beyond what the tests mock
- Legal status of the reverse-engineered protocol (Frida notes in `PROTOCOL.md`)
- HACS publication / HA brands assets (CI already ignores `topics` and `brands`)
