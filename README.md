# Finder YOU — Home Assistant integration

Control your **Finder YESLY roller shutters** from Home Assistant via the
Finder YOU cloud — no iPhone, no Android, no Alexa-routine hack.

> Status: v0.1.0. Shutters only. Position feedback and scenes land in v0.2.

## What it does

Adds a `cover.*` entity for every roller shutter on your Finder YOU plant.
Supports open, close, and set-position. The integration mints fresh tokens
from your email + password and maintains a long-lived HTTP/2 connection to
the Finder cloud so commands reach your gateway promptly.

## Install (via HACS)

1. HACS → ⋮ → Custom repositories
2. URL: `https://github.com/7AC/homeassistant-finder-you`
3. Category: Integration
4. Install. Restart Home Assistant.
5. Settings → Devices & Services → Add Integration → Finder YOU.
6. Enter the same email + password you use for the Finder YOU app.

## Compatibility

- Tested with Finder YOU app **v1.4.4** cloud.
- Account must **own** (not just share) the plant.
- YESLY roller shutters via a **1Y.GU** gateway only.
  Bliss thermostats use the separate
  [`condatek/finderblissha`](https://github.com/condatek/finderblissha).
- Single plant per account.

## Known limits in v0.1

- **No native scenarios** — control individual blinds and build
  "all open" / "all closed" macros with Apple Home scenes or HA
  automations. Concurrent fan-outs are serialized internally with a
  small inter-command gap so the YESLY gateway doesn't drop commands
  under burst load.
- **Position lags after a command** — the cloud plant-state cache
  takes 30–60 s to reflect a command, so the cover reports the
  commanded target during that window and reconciles with the
  observed position afterwards. Wall-switch / app changes still
  propagate at the scan interval.

---

## Protocol

The cloud protocol is documented in [PROTOCOL.md](PROTOCOL.md) — OAuth,
the HTTP/2 connection setup, the gRPC method shapes, and the
three-message `OpenNotificationChannel` subscription that the cloud
demands before it will accept device-touching calls.

## Credits

- [`hpack`](https://pypi.org/project/hpack/) for HPACK encode/decode.
- [`condatek/finderblissha`](https://github.com/condatek/finderblissha)
  for the Bliss-side reverse engineering that proved the broader Finder
  cloud was tractable.

---

## Integrazione Ibrida: Local-First BLE via BLE_Finder_Yesly

Questa integrazione supporta l'operatività combinata con il progetto compagno [`BLE_Finder_Yesly`](https://github.com/francescodoffizi/BLE_Finder_Yesly):
- **Local-First Routing**: Invia comandi diretti ultra-veloci (0 latenza, offline) via Bluetooth Proxy / ESPHome ai singoli nodi Yesly (`13S2`, `1322`).
- **Cloud Fallback Automatico**: Se il segnale radio BLE locale non risponde o il proxy non è raggiungibile, il coordinator instrada istantaneamente il comando attraverso le API Finder Cloud gRPC/HTTP2 ufficiali.
- **Topologia Condivisa**: Tramite `topology.py`, i mapping UUID <-> MAC address e le chiavi cifrate AES dell'impianto vengono usati in sinergia per il controllo radio diretto.

## License

MIT.

