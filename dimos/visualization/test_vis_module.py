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

"""vis_module bundling: the relay bridge rides in iff the relay is enabled."""

from dimos.core.global_config import global_config
from dimos.visualization.vis_module import vis_module
from dimos.web.relay_bridge.relay_bridge_module import RelayBridgeModule


def _modules(bundle) -> set[type]:
    return {atom.module for atom in bundle.blueprints}


def test_relay_bridge_absent_by_default() -> None:
    assert RelayBridgeModule not in _modules(vis_module("none"))


def test_local_relay_flag_appends_relay_bridge(monkeypatch) -> None:
    monkeypatch.setattr(global_config, "local_relay", True)
    assert RelayBridgeModule in _modules(vis_module("none"))


def test_relay_url_appends_relay_bridge(monkeypatch) -> None:
    monkeypatch.setattr(global_config, "relay_url", "https://relay.example:443")
    assert RelayBridgeModule in _modules(vis_module("none"))
