"""Async raw HTTP/2 gRPC client for Finder YOU's you-api.

Why raw HTTP/2 instead of grpc-python: the server rejects requests with
``error 19`` when the gRPC client opens fresh connections per call. We have to
- send Android's exact SETTINGS frame (ENABLE_PUSH=0, INITIAL_WINDOW_SIZE=65535),
- send a connection-level WINDOW_UPDATE of ~67 MB,
- hold OpenNotificationChannel open on a long-lived stream,
- run every other RPC on the same TCP+TLS+HTTP/2 connection
to be accepted as the "live" client by the gateway router.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import struct
import uuid

import hpack

from .proto import build_client_info, field_string, field_varint, parse_fields

_LOGGER = logging.getLogger(__name__)

YOU_API_HOST = "you-api.iot.findernet.com"
YOU_API_PORT = 443
SERVICE = "/finder_home.grpc.common.model.v1.FinderHome"
USER_AGENT = "grpc-dotnet/2.66.0 (.NET 9.0.14; CLR 9.0.14; net8.0; arm64)"

# HTTP/2 framing constants
H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
TYPE_DATA = 0
TYPE_HEADERS = 1
TYPE_SETTINGS = 4
TYPE_PING = 6
TYPE_GOAWAY = 7
TYPE_WINDOW_UPDATE = 8
FLAG_END_STREAM = 0x01
FLAG_END_HEADERS = 0x04
FLAG_ACK = 0x01

# Settings IDs
SETTINGS_ENABLE_PUSH = 0x02
SETTINGS_INITIAL_WINDOW_SIZE = 0x04

# Android's connection-level WINDOW_UPDATE increment (~67 MB)
ANDROID_CONN_WINDOW = 0x03FF0001


class FinderApiError(Exception):
    """Raised when an RPC returns a non-zero status code."""

    def __init__(self, method: str, status: int, code: int | None = None) -> None:
        super().__init__(f"{method}: status={status} code={code}")
        self.method = method
        self.status = status
        self.code = code


class GatewayOfflineError(Exception):
    """Raised when the cloud acked a command but the gateway never reported
    back the corresponding state-change notification within the timeout.

    The cloud's HTTP/2 reply is just routing-layer success; the YESLY gateway
    in the user's home is responsible for actually moving the shutter and
    pushing a state notification back over OpenNotificationChannel. When the
    gateway's MQTT link is dead, the cloud still acks 0801 but the shutter
    never moves -- the only way to detect this is the missing notification.
    """


def _build_frame(ftype: int, flags: int, sid: int, body: bytes) -> bytes:
    return struct.pack(">L", len(body))[1:] + bytes([ftype, flags]) + struct.pack(">L", sid) + body


class FinderHomeClient:
    """Persistent async HTTP/2 connection to Finder YOU's gRPC service.

    Lifetime: open() once, run() forever, call rpcs, close() on shutdown.
    Use ``async with`` for convenience.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._encoder = hpack.Encoder()
        self._decoder = hpack.Decoder()
        # Match Android's stream-ID pattern: OpenNotificationChannel goes on
        # stream 1 (first stream of the connection); bootstrap RPCs follow on
        # 3, 5, 7…
        self._next_stream_id = 1
        self._stream_id_lock = asyncio.Lock()
        # client_uuid generated per integration install; not security-relevant
        # to the server but used in ClientInfo.
        self._client_uuid = str(uuid.uuid4()).encode()
        self._client_info = build_client_info(self._client_uuid)
        # Pending responses: stream_id -> Future of (body, grpc_status)
        self._pending: dict[int, asyncio.Future] = {}
        self._notification_queue: asyncio.Queue = asyncio.Queue()
        self._notification_stream_id: int | None = None
        self._read_task: asyncio.Task | None = None
        self._notification_keepalive_task: asyncio.Task | None = None
        # shutter_uuid (str) -> list of waiters to resolve next time the cloud
        # streams back a notification mentioning that shutter.
        self._shutter_waiters: dict[str, list[asyncio.Future]] = {}

    @classmethod
    async def connect(cls, token: str) -> FinderHomeClient:
        """Open the connection and run the Android-style handshake."""
        self = cls(token)
        await self._connect()
        return self

    async def _connect(self) -> None:
        # Pre-build the SSLContext in a thread executor — create_default_context()
        # does blocking disk I/O (set_default_verify_paths) which HA forbids on
        # the event loop.
        loop = asyncio.get_running_loop()

        def _make_ctx() -> ssl.SSLContext:
            c = ssl.create_default_context()
            c.set_alpn_protocols(["h2"])
            return c

        ssl_ctx = await loop.run_in_executor(None, _make_ctx)
        self._reader, self._writer = await asyncio.open_connection(
            YOU_API_HOST, YOU_API_PORT, ssl=ssl_ctx, server_hostname=YOU_API_HOST
        )

        # Android's exact handshake bytes:
        #   * preface
        #   * SETTINGS { ENABLE_PUSH=0, INITIAL_WINDOW_SIZE=65535 }
        #   * WINDOW_UPDATE +ANDROID_CONN_WINDOW on connection
        settings_body = struct.pack(
            ">HI HI", SETTINGS_ENABLE_PUSH, 0, SETTINGS_INITIAL_WINDOW_SIZE, 0xFFFF
        )
        self._writer.write(
            H2_PREFACE
            + _build_frame(TYPE_SETTINGS, 0, 0, settings_body)
            + _build_frame(TYPE_WINDOW_UPDATE, 0, 0, struct.pack(">L", ANDROID_CONN_WINDOW))
        )
        await self._writer.drain()

        # Read the first few frames to catch the server's SETTINGS and ACK it.
        # We don't strictly need the SETTINGS body; just ACK.
        for _ in range(3):
            frame = await self._recv_frame()
            if frame is None:
                raise ConnectionError("server closed before handshake")
            ftype, flags, sid, body = frame
            if ftype == TYPE_SETTINGS and not (flags & FLAG_ACK):
                # Send our ACK
                self._writer.write(_build_frame(TYPE_SETTINGS, FLAG_ACK, 0, b""))
                await self._writer.drain()
                break

        self._read_task = asyncio.create_task(self._read_loop())

    async def _recv_frame(self) -> tuple[int, int, int, bytes] | None:
        assert self._reader is not None
        hdr = await self._reader.readexactly(9)
        length = int.from_bytes(hdr[0:3], "big")
        ftype = hdr[3]
        flags = hdr[4]
        sid = int.from_bytes(hdr[5:9], "big") & 0x7FFFFFFF
        body = await self._reader.readexactly(length) if length else b""
        return ftype, flags, sid, body

    async def _read_loop(self) -> None:
        """Continuously read frames and dispatch to pending stream futures."""
        # Per-stream buffers for DATA frames and trailers state.
        data_bufs: dict[int, bytearray] = {}
        statuses: dict[int, int] = {}
        try:
            while True:
                try:
                    frame = await self._recv_frame()
                except (asyncio.IncompleteReadError, ConnectionError):
                    break
                ftype, flags, sid, body = frame

                if ftype == TYPE_DATA:
                    if sid == self._notification_stream_id:
                        # OpenNotificationChannel stream — every DATA frame
                        # is a server-streamed message.
                        # gRPC framing prefix (5B) is part of each message.
                        if len(body) >= 5:
                            msg_len = int.from_bytes(body[1:5], "big")
                            msg = body[5 : 5 + msg_len]
                            self._notification_queue.put_nowait(msg)
                            self._dispatch_shutter_event(msg)
                    else:
                        buf = data_bufs.setdefault(sid, bytearray())
                        buf.extend(body)
                        if flags & FLAG_END_STREAM:
                            self._finalize_stream(sid, bytes(buf), statuses.get(sid, 0))
                            data_bufs.pop(sid, None)
                            statuses.pop(sid, None)
                elif ftype == TYPE_HEADERS:
                    try:
                        for k, v in self._decoder.decode(body):
                            if isinstance(k, bytes):
                                k = k.decode()
                            if isinstance(v, bytes):
                                v = v.decode()
                            if k == "grpc-status":
                                statuses[sid] = int(v)
                    except Exception:
                        _LOGGER.exception("decode trailers")
                    if flags & FLAG_END_STREAM:
                        buf = data_bufs.pop(sid, bytearray())
                        self._finalize_stream(sid, bytes(buf), statuses.pop(sid, 0))
                elif ftype == TYPE_PING and not (flags & FLAG_ACK):
                    # Echo back as ACK
                    assert self._writer is not None
                    self._writer.write(_build_frame(TYPE_PING, FLAG_ACK, 0, body))
                    await self._writer.drain()
                elif ftype == TYPE_GOAWAY:
                    _LOGGER.warning("server sent GOAWAY")
                    break
        finally:
            # Fail any still-pending RPCs.
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("h2 connection closed"))
            self._pending.clear()

    def _finalize_stream(self, sid: int, body: bytes, status: int) -> None:
        fut = self._pending.pop(sid, None)
        if fut is not None and not fut.done():
            # Strip the 5-byte gRPC framing prefix from the body
            payload = body[5:] if len(body) >= 5 else b""
            fut.set_result((payload, status))

    def _dispatch_shutter_event(self, msg: bytes) -> None:
        """Scan a notification for UUIDs and wake any waiter for that shutter.

        The cloud streams shutter state-change events as nested protobufs
        embedded in field 5 of the notification body. We walk anything that
        looks length-delimited and resolve futures for any UUID-shaped
        string we find -- belt-and-braces, since the exact field structure
        is opaque and may differ between OpenFull / CloseFull / SetOpenPercent.
        """
        try:
            seen: set[str] = set()
            stack = [msg]
            while stack:
                buf = stack.pop()
                try:
                    fields = parse_fields(buf)
                except Exception:
                    continue
                for vals in fields.values():
                    for v in vals:
                        if not isinstance(v, bytes):
                            continue
                        if len(v) == 36 and v.count(b"-") == 4:
                            seen.add(v.decode("ascii", errors="ignore"))
                        elif 2 <= len(v) <= 4096:
                            stack.append(v)
            for uuid_str in seen:
                waiters = self._shutter_waiters.pop(uuid_str, None)
                if not waiters:
                    continue
                for fut in waiters:
                    if not fut.done():
                        fut.set_result(None)
        except Exception:
            _LOGGER.exception("dispatch_shutter_event failed")

    async def wait_for_shutter_event(self, shutter_id: bytes, timeout: float) -> None:
        """Block until the gateway pushes a notification mentioning shutter_id.

        Raises ``GatewayOfflineError`` if no notification arrives within
        ``timeout``. Use this right after a command call to verify the
        gateway actually relayed the action (rather than the cloud just
        acking it into the void).
        """
        key = shutter_id.decode("ascii") if isinstance(shutter_id, bytes) else shutter_id
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._shutter_waiters.setdefault(key, []).append(fut)
        try:
            await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError as err:
            # Clean up our entry so a late notification doesn't pop a stale future.
            waiters = self._shutter_waiters.get(key)
            if waiters and fut in waiters:
                waiters.remove(fut)
                if not waiters:
                    self._shutter_waiters.pop(key, None)
            raise GatewayOfflineError(
                f"no gateway notification for {key} within {timeout}s"
            ) from err

    async def _next_sid(self) -> int:
        async with self._stream_id_lock:
            sid = self._next_stream_id
            self._next_stream_id += 2
            return sid

    def _build_headers_frame(self, method: str, sid: int, with_auth: bool) -> bytes:
        headers = [
            (":authority", YOU_API_HOST),
            (":method", "POST"),
            (":path", f"{SERVICE}/{method}"),
            (":scheme", "https"),
            ("user-agent", USER_AGENT),
            ("te", "trailers"),
            ("grpc-accept-encoding", "identity,gzip,deflate"),
        ]
        if with_auth:
            headers.append(("authorization", f"Bearer {self._token}"))
        headers.append(("content-type", "application/grpc"))
        encoded = self._encoder.encode(headers)
        return _build_frame(TYPE_HEADERS, FLAG_END_HEADERS, sid, encoded)

    def _build_data_frame(self, body: bytes, sid: int, end_stream: bool = True) -> bytes:
        grpc_body = bytes([0]) + struct.pack(">L", len(body)) + body
        flags = FLAG_END_STREAM if end_stream else 0
        return _build_frame(TYPE_DATA, flags, sid, grpc_body)

    async def _call(self, method: str, body: bytes, with_auth: bool = True) -> bytes:
        assert self._writer is not None
        sid = await self._next_sid()
        fut: asyncio.Future[tuple[bytes, int]] = asyncio.get_event_loop().create_future()
        self._pending[sid] = fut
        self._writer.write(
            self._build_headers_frame(method, sid, with_auth) + self._build_data_frame(body, sid)
        )
        await self._writer.drain()
        payload, status = await asyncio.wait_for(fut, timeout=15)
        if status != 0:
            raise FinderApiError(method, status)
        # The application also encodes a status in field 1 of every response.
        # 0x08 0x01 (varint 1) = OK, 0x08 0x02 = error follows.
        fields = parse_fields(payload)
        if fields.get(1, [0])[0] == 2:
            code = fields.get(2, [0])[0]
            raise FinderApiError(method, 2, code)
        return payload

    # ===== High-level RPCs ============================================

    async def handshake(self) -> dict:
        """Run the Android-style boot sequence: CheckUser, PlatformCheck,
        GetUserPlants, OpenNotificationChannel (held open). Returns the
        GetUserPlants response.

        CheckUser is sent without auth on stream 3 and is what makes the
        server's session accept later device-touching calls — skipping it
        causes GetPlant/SetOpenPercent to fail with ``code 1``.
        """
        # The Android app opens OpenNotificationChannel **first** — before
        # the bootstrap RPCs — on stream 1. It then does a three-step
        # subscription handshake on that stream, after which the server
        # considers it the "live" client and routes gateway commands to it.
        # Skip this and every device-touching call (GetPlant, SetOpenPercent,
        # etc.) fails with code 19. Order matters: bootstrap RPCs go on
        # later streams (3, 5, 7…) AFTER the OpenNot subscription is
        # established.
        assert self._writer is not None
        sid = await self._next_sid()
        self._notification_stream_id = sid
        ci_payload = field_string(1, self._client_info)
        # Message 1: hello, here's who I am (server replies "10 01")
        self._writer.write(
            self._build_headers_frame("OpenNotificationChannel", sid, with_auth=True)
            + self._build_data_frame(ci_payload, sid, end_stream=False)
        )
        await self._writer.drain()
        try:
            await asyncio.wait_for(self._notification_queue.get(), timeout=3)
        except TimeoutError:
            _LOGGER.warning("no claim ack on first notification msg within 3s")

        # Send a PING — Android does this between msg 1 and msg 2.
        ping_data = struct.pack(">Q", 0xFFFF_FFFF_FFFF_FFFF)
        self._writer.write(
            struct.pack(">L", 8)[1:] + bytes([TYPE_PING, 0]) + struct.pack(">L", 0) + ping_data
        )
        await self._writer.drain()
        await asyncio.sleep(0.1)

        # Message 2: subscribe-as-client {field 1: ClientInfo, field 2: 1}.
        # Server replies "10 01 40 01" — the field 8 (= 0x40) addition is
        # the "you're now the authoritative client" signal.
        subscribe_client = ci_payload + field_varint(2, 1)
        self._writer.write(self._build_data_frame(subscribe_client, sid, end_stream=False))
        await self._writer.drain()
        try:
            resp = await asyncio.wait_for(self._notification_queue.get(), timeout=3)
            _LOGGER.info("subscribe-client response: %s", resp.hex())
        except TimeoutError:
            _LOGGER.warning("no claim ack on subscribe-client within 3s")

        # Now run the bootstrap RPCs. Per the Frida-captured Android flow,
        # the app does these AFTER the OpenNot subscription is granted.
        await self._call("CheckUser", ci_payload, with_auth=False)
        await self._call("PlatformCheck", ci_payload, with_auth=False)
        plants_resp = await self._call("GetUserPlants", ci_payload)
        plants_msg = parse_fields(plants_resp)

        # Message 3: subscribe-plant {field 1: ClientInfo, field 2: 2, field 3: plant_id}.
        # Picks the plant out of the GetUserPlants response.
        if 3 in plants_msg:
            plant_inner = parse_fields(plants_msg[3][0])
            plant_id = plant_inner.get(1, [b""])[0]
            if plant_id:
                # Android wraps plant_id one level deeper: field 3 of the
                # subscribe-plant message is itself a message whose inner
                # field 1 is the plant_id string.
                plant_envelope = field_string(1, plant_id)
                subscribe_plant = ci_payload + field_varint(2, 2) + field_string(3, plant_envelope)
                self._writer.write(self._build_data_frame(subscribe_plant, sid, end_stream=False))
                await self._writer.drain()
                # Wait for plant-subscribe response — Android receives a
                # large plant-state payload after this; we just need to know
                # the gateway has acked routing.
                try:
                    resp = await asyncio.wait_for(self._notification_queue.get(), timeout=5)
                    _LOGGER.info("subscribe-plant response (%d B): %s", len(resp), resp[:60].hex())
                except TimeoutError:
                    _LOGGER.warning("no subscribe-plant response within 5s")

        # Keepalive: re-send subscribe-client every 30 s to hold the claim.
        self._notification_keepalive_task = asyncio.create_task(
            self._notification_keepalive(sid, subscribe_client)
        )
        return plants_msg

    async def _notification_keepalive(self, sid: int, keepalive_body: bytes) -> None:
        """Re-send the subscribe-client message every 30 s so the cloud
        keeps us as the authoritative live client."""
        try:
            while True:
                await asyncio.sleep(30)
                if self._writer is None:
                    return
                self._writer.write(self._build_data_frame(keepalive_body, sid, end_stream=False))
                await self._writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.exception("notification keepalive crashed")

    async def get_plant(self, plant_id: bytes) -> bytes:
        return await self._call(
            "GetPlant", field_string(1, self._client_info) + field_string(2, plant_id)
        )

    async def set_open_percent(self, plant_id: bytes, shutter_id: bytes, percent: int) -> None:
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, shutter_id)
            + field_varint(4, percent)
        )
        await self._call("SetOpenPercent", body)

    async def open_full(self, plant_id: bytes, shutter_id: bytes) -> None:
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, shutter_id)
        )
        await self._call("OpenFull", body)

    async def close_full(self, plant_id: bytes, shutter_id: bytes) -> None:
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, shutter_id)
        )
        await self._call("CloseFull", body)

    async def activate_scenario(self, plant_id: bytes, scenario_id: bytes) -> None:
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, scenario_id)
        )
        await self._call("ActivateScenario", body)

    async def switch_on(self, plant_id: bytes, device_id: bytes, channel: int = 1) -> None:
        channel_sub = field_varint(1, channel)
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, device_id)
            + field_string(4, channel_sub)
        )
        await self._call("SwitchOn", body)

    async def switch_off(self, plant_id: bytes, device_id: bytes, channel: int = 1) -> None:
        channel_sub = field_varint(1, channel)
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, device_id)
            + field_string(4, channel_sub)
        )
        await self._call("SwitchOff", body)

    async def set_brightness(
        self, plant_id: bytes, device_id: bytes, percent: int, channel: int = 1
    ) -> None:
        channel_sub = field_varint(1, channel)
        body = (
            field_string(1, self._client_info)
            + field_string(2, plant_id)
            + field_string(3, device_id)
            + field_varint(4, percent)
            + field_string(5, channel_sub)
        )
        await self._call("SetBrightness", body)

    async def close(self) -> None:

        if self._notification_keepalive_task:
            self._notification_keepalive_task.cancel()
        if self._read_task:
            self._read_task.cancel()
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

    async def __aenter__(self) -> FinderHomeClient:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
