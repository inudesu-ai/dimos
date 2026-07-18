# Copyright 2026 Dimensional Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""RelayBridgeModule unit tests: no network, no Deno, no LCM.

A fake relay client is injected under `connect_with_backoff` and fake
transports under the module's `In` streams, so lazy subscribe/unsubscribe,
the maxHz gate, the encode path, and reconnect are all observable directly.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
import json
import socket
import time
from typing import Any

import numpy as np
import pytest

from dimos.msgs.geometry_msgs.PoseStamped import PoseStamped
from dimos.msgs.sensor_msgs.Image import Image
from dimos.web.relay_bridge import relay_bridge_module
from dimos.web.relay_bridge.protocol import Msg, RobotManifest, Subs
from dimos.web.relay_bridge.relay_bridge_module import (
    RelayBridgeConfig,
    RelayBridgeModule,
    build_manifest,
    resolve_robot_info,
)


class FakeWriter:
    def __init__(self) -> None:
        self.offers: list[tuple[bytes, dict[str, Any] | None]] = []

    def offer(self, payload: bytes, meta: dict[str, Any] | None = None) -> None:
        self.offers.append((payload, meta))


class FakeClient:
    """Duck-typed RelayClient: everything the module touches, nothing else."""

    def __init__(self) -> None:
        self.hello_args: tuple[Any, Any] | None = None
        self.control_msgs: asyncio.Queue[Msg] = asyncio.Queue()
        self.closed = asyncio.Event()
        self.writers: dict[str, FakeWriter] = {}
        self.frames: list[tuple[str, bytes, str, dict[str, Any] | None]] = []
        self.close_count = 0

    async def hello(self, timeout: float = 5.0, *, robot: Any = None, manifest: Any = None) -> None:
        self.hello_args = (robot, manifest)

    def latest_writer(self, ch: str, *, stale_after: float = 0.5) -> FakeWriter:
        writer = FakeWriter()
        self.writers[ch] = writer
        return writer

    def send_frame(
        self,
        ch: str,
        payload: bytes,
        *,
        delivery: str = "reliable",
        meta: dict[str, Any] | None = None,
        ts: float | None = None,
    ) -> int:
        self.frames.append((ch, bytes(payload), delivery, meta))
        return 1

    async def control_messages(self) -> AsyncIterator[Msg]:
        while True:
            get = asyncio.ensure_future(self.control_msgs.get())
            closed = asyncio.ensure_future(self.closed.wait())
            try:
                done, _ = await asyncio.wait({get, closed}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                closed.cancel()
                if not get.done():
                    get.cancel()
            if get in done:
                yield get.result()
                continue
            return

    async def close(self) -> None:
        self.close_count += 1
        self.closed.set()


class FakeTransport:
    """In-stream transport stub: counts subscribers, publishes synchronously
    (the test thread plays the LCM callback thread)."""

    def __init__(self) -> None:
        self.subscribers: list[Callable[[Any], Any]] = []
        self.unsubscribed = 0

    def subscribe(self, cb: Callable[[Any], Any], stream: Any = None) -> Callable[[], None]:
        self.subscribers.append(cb)

        def unsubscribe() -> None:
            self.subscribers.remove(cb)
            self.unsubscribed += 1

        return unsubscribe

    def publish(self, msg: Any) -> None:
        for cb in list(self.subscribers):
            cb(msg)

    def stop(self) -> None:  # called by In.stop() during module close
        self.subscribers.clear()


def wait_until(cond: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


@pytest.fixture
def bridge(monkeypatch):
    clients: list[FakeClient] = []

    async def fake_connect(url: str, role: str, **kwargs: Any) -> FakeClient:
        clients.append(FakeClient())
        return clients[-1]

    monkeypatch.setattr(relay_bridge_module, "connect_with_backoff", fake_connect)
    module = RelayBridgeModule(
        relay_url="https://127.0.0.1:1", open_browser=False, robot_id="unit-bot"
    )
    module.color_image.transport = FakeTransport()
    module.odom.transport = FakeTransport()
    module.start()
    try:
        yield module, clients
    finally:
        module.stop()


def push(module: RelayBridgeModule, client: FakeClient, msg: Msg) -> None:
    """Deliver a relay push onto the module's own event loop (queue affinity)."""
    assert module._loop is not None
    module._loop.call_soon_threadsafe(client.control_msgs.put_nowait, msg)


def kill_session(module: RelayBridgeModule, client: FakeClient) -> None:
    assert module._loop is not None
    module._loop.call_soon_threadsafe(client.closed.set)


def image_transport(module: RelayBridgeModule) -> FakeTransport:
    transport = module.color_image.transport
    assert isinstance(transport, FakeTransport)
    return transport


def odom_transport(module: RelayBridgeModule) -> FakeTransport:
    transport = module.odom.transport
    assert isinstance(transport, FakeTransport)
    return transport


def test_manifest_and_robot_info_content() -> None:
    config = RelayBridgeConfig(robot_id="go2-lab", robot_name="Lab", image_max_hz=12.0)
    manifest = build_manifest(config)
    assert [c.ch for c in manifest.channels] == ["color_image", "odom"]
    image, odom = manifest.channels
    assert (image.encoding, image.delivery, image.maxHz) == ("jpeg.v1", "latest", 12.0)
    assert (odom.encoding, odom.delivery, odom.maxHz) == ("pose.json.v1", "reliable", 20.0)

    info = resolve_robot_info(config)
    assert (info.id, info.name) == ("go2-lab", "Lab")
    # Fallback chain: explicit id -> global robot_id -> hostname.
    fallback = resolve_robot_info(RelayBridgeConfig())
    assert fallback.id == (RelayBridgeConfig().g.robot_id or socket.gethostname())
    assert fallback.name == fallback.id


def test_start_registers_but_subscribes_nothing(bridge) -> None:
    # Lazy-encode guard: if someone ever adds handle_color_image/handle_odom,
    # _auto_bind_handlers would eagerly subscribe here and this fails.
    module, clients = bridge
    robot, manifest = clients[0].hello_args
    assert robot.id == "unit-bot"
    assert isinstance(manifest, RobotManifest) and len(manifest.channels) == 2
    assert image_transport(module).subscribers == []
    assert odom_transport(module).subscribers == []


def test_subs_snapshot_toggles_subscriptions(bridge) -> None:
    module, clients = bridge
    push(module, clients[0], Subs(chs=["odom"], n=1))
    assert wait_until(lambda: len(odom_transport(module).subscribers) == 1)
    assert image_transport(module).subscribers == []

    # A stale (already-seen n) snapshot must be ignored.
    push(module, clients[0], Subs(chs=[], n=1))
    time.sleep(0.1)
    assert len(odom_transport(module).subscribers) == 1

    push(module, clients[0], Subs(chs=[], n=2))
    assert wait_until(lambda: odom_transport(module).subscribers == [])


def test_unknown_channels_in_snapshot_are_ignored(bridge) -> None:
    module, clients = bridge
    push(module, clients[0], Subs(chs=["mystery", "odom"], n=1))
    assert wait_until(lambda: len(odom_transport(module).subscribers) == 1)
    assert image_transport(module).subscribers == []


def test_encode_paths_and_max_hz_gate(bridge) -> None:
    module, clients = bridge
    client = clients[0]
    push(module, client, Subs(chs=["color_image", "odom"], n=1))
    assert wait_until(
        lambda: image_transport(module).subscribers and odom_transport(module).subscribers
    )

    pose = PoseStamped(ts=42.5, position=[1.5, -2.5, 0.25], orientation=[0.0, 0.0, 0.0, 1.0])
    odom_transport(module).publish(pose)
    assert module.encoded["odom"] == 1
    assert wait_until(lambda: len(client.frames) == 1)
    ch, payload, delivery, _ = client.frames[0]
    assert (ch, delivery) == ("odom", "reliable")
    decoded = json.loads(payload)
    assert decoded == {"x": 1.5, "y": -2.5, "z": 0.25, "yaw": 0.0, "ts": 42.5}

    image = Image.from_numpy(np.zeros((8, 12, 3), dtype=np.uint8))
    image_transport(module).publish(image)
    assert module.encoded["color_image"] == 1
    assert wait_until(lambda: client.writers["color_image"].offers)
    jpeg, meta = client.writers["color_image"].offers[0]
    assert jpeg[:2] == b"\xff\xd8"  # JPEG magic: TurboJPEG really encoded
    assert meta == {"w": 12, "h": 8}

    # maxHz gate: the first publish warmed the encode path (lazy imports cost
    # ~250 ms), so this back-to-back pair reliably lands inside the 50 ms
    # interval - exactly one of the two encodes.
    time.sleep(0.06)
    count = module.encoded["odom"]
    odom_transport(module).publish(pose)
    odom_transport(module).publish(pose)
    assert module.encoded["odom"] == count + 1

    # After the interval passes, encoding resumes.
    time.sleep(0.06)
    odom_transport(module).publish(pose)
    assert wait_until(lambda: module.encoded["odom"] == count + 2)


def test_session_loss_stops_encoders_and_reconnects(bridge) -> None:
    module, clients = bridge
    push(module, clients[0], Subs(chs=["odom"], n=5))
    assert wait_until(lambda: odom_transport(module).subscribers)

    kill_session(module, clients[0])
    # Encoders stop the moment the session dies (no consumer = no work) ...
    assert wait_until(lambda: odom_transport(module).subscribers == [])
    # ... and the supervisor dials a fresh session with a reset n horizon.
    assert wait_until(lambda: len(clients) == 2)
    push(module, clients[1], Subs(chs=["odom"], n=1))
    assert wait_until(lambda: len(odom_transport(module).subscribers) == 1)


def test_stop_unsubscribes_and_closes(bridge) -> None:
    module, clients = bridge
    push(module, clients[0], Subs(chs=["color_image", "odom"], n=1))
    assert wait_until(lambda: image_transport(module).subscribers)

    image_tr, odom_tr = image_transport(module), odom_transport(module)
    module.stop()
    # The module's own teardown unsubscribed (not just the transports closing).
    assert (image_tr.unsubscribed, odom_tr.unsubscribed) == (1, 1)
    assert image_tr.subscribers == [] and odom_tr.subscribers == []
    assert clients[0].close_count >= 1
