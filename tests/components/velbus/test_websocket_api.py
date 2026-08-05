"""Tests for the Velbus config panel websocket API."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
import pytest
from velbusaio.action_cache import ActionScan, ModuleActions, ScanProgress
from velbusaio.actions import ActionSlot
from velbusaio.config import ConfigParameter

from homeassistant.components.velbus.const import CONF_ADVANCED_MODE
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from . import init_integration

from tests.common import MockConfigEntry
from tests.typing import WebSocketGenerator

BUS_SETTING = "light_autosend_interval"
MEMORY_SETTING = "name"


def _param(key: str, *, writes_memory: bool) -> ConfigParameter:
    """Return a configuration parameter with a stubbed setter."""
    return ConfigParameter(
        key=key,
        label=key,
        kind="number",
        getter=AsyncMock(return_value=60),
        setter=AsyncMock(),
        channel=0,
        min_value=0.0,
        max_value=255.0,
        writes_memory=writes_memory,
    )


@pytest.fixture(name="params")
def params_fixture(controller: MagicMock) -> dict[str, ConfigParameter]:
    """Give the mocked module one memory parameter and one bus parameter."""
    params = {
        MEMORY_SETTING: _param(MEMORY_SETTING, writes_memory=True),
        BUS_SETTING: _param(BUS_SETTING, writes_memory=False),
    }
    module = controller.return_value.get_module.return_value
    module.find_config_parameter.side_effect = lambda key, channel=None: params.get(key)
    return params


async def _set_config(
    hass_ws_client: WebSocketGenerator,
    hass: HomeAssistant,
    entry: MockConfigEntry,
    key: str,
) -> dict:
    """Call module/config/set for one key and return the response."""
    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/module/config/set",
            "config_entry": entry.entry_id,
            "address": 1,
            "channel": 0,
            "key": key,
            "value": 60,
        }
    )
    return await client.receive_json()


@pytest.mark.parametrize(
    ("key", "advanced_mode", "succeeds"),
    [
        pytest.param(BUS_SETTING, False, True, id="bus_setting_without_advanced_mode"),
        pytest.param(
            MEMORY_SETTING, False, False, id="memory_setting_without_advanced_mode"
        ),
        pytest.param(
            MEMORY_SETTING, True, True, id="memory_setting_with_advanced_mode"
        ),
    ],
)
async def test_advanced_mode_guards_only_memory_writes(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    params: dict[str, ConfigParameter],
    key: str,
    advanced_mode: bool,
    succeeds: bool,
) -> None:
    """Advanced mode guards eeprom writes, not settings sent over the bus.

    A bus message cannot corrupt anything: the module either understands it or
    ignores it, unlike a write to an eeprom address that may be wrong.
    """
    hass.config_entries.async_update_entry(
        config_entry, data={**config_entry.data, CONF_ADVANCED_MODE: advanced_mode}
    )
    await init_integration(hass, config_entry)

    response = await _set_config(hass_ws_client, hass, config_entry, key)

    assert response["success"] is succeeds
    assert params[key].setter.await_count == int(succeeds)


@pytest.mark.usefixtures("params")
async def test_unknown_key(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """An unknown key names the channel it was looked for on."""
    await init_integration(hass, config_entry)

    response = await _set_config(hass_ws_client, hass, config_entry, "nope")

    assert not response["success"]
    assert "nope" in response["error"]["message"]
    assert "channel 0" in response["error"]["message"]


async def test_modules_report_the_bus_location_of_each_channel(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    controller: MagicMock,
) -> None:
    """Channels above eight live on a subaddress and are numbered from one there."""
    modules = controller.return_value.get_modules.return_value
    for item in modules.values():
        item.get_autosend_kinds.return_value = []
        item.get_temp_settings.return_value = None
        item.supports_temperature.return_value = False
        item.get_properties.return_value = {}
        item.get_memory_map_build.return_value = "1915"
        item.get_type.return_value = 42
        item.is_memory_map_outdated.return_value = False
    module = modules[99]
    module.get_address.return_value = 88
    module.get_addresses.return_value = [88]
    module.get_sub_address_dict.return_value = {1: 89, 3: 91}
    module.get_channels.return_value = {
        number: MagicMock(get_name=MagicMock(return_value=f"channel {number}"))
        for number in (1, 9, 17, 25)
    }
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/modules",
            "config_entry": config_entry.entry_id,
        }
    )
    response = await client.receive_json()

    assert response["success"]
    channels = next(
        item for item in response["result"]["modules"] if item["address"] == 88
    )["channels"]
    assert {
        number: (info["bus_address"], info["bus_channel"])
        for number, info in channels.items()
    } == {
        "1": (88, 1),
        "9": (89, 1),
        # Without a subaddress of its own the channel stays on the module.
        "17": (88, 17),
        "25": (91, 1),
    }


async def test_a_channel_cannot_be_its_own_action_source(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    controller: MagicMock,
) -> None:
    """Programming a channel to react to itself would feed its output back in."""
    hass.config_entries.async_update_entry(
        config_entry, data={**config_entry.data, CONF_ADVANCED_MODE: True}
    )
    module = controller.return_value.get_module.return_value
    module.get_address.return_value = 88
    module.get_sub_address_dict.return_value = {}
    relay = MagicMock()
    relay.set_action = AsyncMock()
    module.get_channels.return_value = {1: relay}
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/module/actions/set",
            "config_entry": config_entry.entry_id,
            "address": 88,
            "channel": 1,
            "source_address": 88,
            "source_channel": 1,
            "action": "on",
        }
    )
    response = await client.receive_json()

    assert not response["success"]
    assert response["error"]["message"] == "A channel cannot be its own action source"
    relay.set_action.assert_not_called()


@pytest.fixture(name="shared_params")
def shared_params_fixture(controller: MagicMock) -> dict[int, ConfigParameter]:
    """Give two modules a module wide bus setting and one a per channel one."""
    params: dict[int, ConfigParameter] = {}
    modules = {}
    for address in (1, 99):
        module = MagicMock()
        module.get_address.return_value = address
        module.get_addresses.return_value = [address]
        module.get_name.return_value = f"Module {address}"
        module.get_autosend_kinds.return_value = []
        module.get_temp_settings.return_value = None
        # Filed under the temperature channel, but there is only one of it, so
        # it is module wide in practice.
        shared = _param(BUS_SETTING, writes_memory=False)
        shared.channel = 33
        params[address] = shared
        module.get_config_parameters.return_value = [
            shared,
            # Once per channel, so it belongs to those channels and not to a
            # page that writes every module at once.
            *(
                ConfigParameter(
                    key="inhibit",
                    label="Inhibit",
                    kind="bool",
                    getter=AsyncMock(return_value=False),
                    setter=AsyncMock(),
                    channel=channel,
                    writes_memory=False,
                )
                for channel in (1, 2)
            ),
            # Writes eeprom, so it is not offered installation wide either.
            _param(MEMORY_SETTING, writes_memory=True),
        ]
        modules[address] = module
    controller.return_value.get_modules.return_value = modules
    return params


@pytest.mark.usefixtures("shared_params")
async def test_only_module_wide_bus_settings_are_shared(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """A per channel or eeprom setting is not offered for the whole installation."""
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/config/shared",
            "config_entry": config_entry.entry_id,
        }
    )
    response = await client.receive_json()

    assert response["success"]
    settings = response["result"]["settings"]
    assert [setting["key"] for setting in settings] == [BUS_SETTING]
    assert [module["address"] for module in settings[0]["modules"]] == [1, 99]
    assert [module["value"] for module in settings[0]["modules"]] == [60, 60]


async def test_writing_a_shared_setting_reports_every_module(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    shared_params: dict[int, ConfigParameter],
) -> None:
    """One module that refuses must not hide what happened to the others."""
    shared_params[99].setter.side_effect = OSError("no answer")
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/config/set_shared",
            "config_entry": config_entry.entry_id,
            "key": BUS_SETTING,
            "value": 120,
        }
    )
    response = await client.receive_json()

    assert response["success"]
    assert response["result"]["results"] == [
        {"address": 1, "name": "Module 1", "success": True, "error": None},
        {"address": 99, "name": "Module 99", "success": False, "error": "no answer"},
    ]
    shared_params[1].setter.assert_awaited_once_with(120)


@pytest.mark.usefixtures("shared_params")
async def test_a_shared_setting_can_be_limited_to_named_modules(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    shared_params: dict[int, ConfigParameter],
) -> None:
    """The panel writes the modules it listed, not whatever exists right now."""
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/config/set_shared",
            "config_entry": config_entry.entry_id,
            "key": BUS_SETTING,
            "value": 30,
            "addresses": [1],
        }
    )
    response = await client.receive_json()

    assert response["success"]
    assert [item["address"] for item in response["result"]["results"]] == [1]
    shared_params[99].setter.assert_not_awaited()


@pytest.mark.parametrize(
    ("side_effect", "succeeds"),
    [(None, True), (OSError("bus is gone"), False)],
    ids=["sent", "connection lost"],
)
async def test_sync_clock_broadcasts_home_assistant_time(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
    controller: MagicMock,
    freezer: FrozenDateTimeFactory,
    side_effect: Exception | None,
    succeeds: bool,
) -> None:
    """The clock every module gets is the one Home Assistant keeps."""
    controller.return_value.sync_clock.side_effect = side_effect
    await init_integration(hass, config_entry)

    client = await hass_ws_client(hass)
    # Only now: the websocket login checks token timestamps against the clock,
    # and moving it first fails the handshake instead of the assertion below.
    freezer.move_to("2026-08-05 12:00:00+02:00")
    await client.send_json_auto_id(
        {
            "type": "velbus/config_panel/sync_clock",
            "config_entry": config_entry.entry_id,
        }
    )
    response = await client.receive_json()

    assert response["success"] is succeeds
    assert controller.return_value.sync_clock.await_args.args[0] == dt_util.now()


def _scan(*, error: str | None = None) -> ActionScan:
    """Return a scan holding one programmed slot on module 1, channel 2."""
    slot = ActionSlot.create(0, source_address=5, source_channel=1, action="toggle")
    scan = ActionScan(duration=1.5)
    scan.modules[1] = ModuleActions(
        address=1,
        name="Relay",
        type_name="VMB4RYLD",
        channels={2: [slot]},
        read_at=1234.0,
        from_cache=True,
        error=error,
    )
    return scan


@pytest.fixture(name="source_module")
def source_module_fixture(controller: MagicMock) -> MagicMock:
    """Make the mocked module answer as the source of an action slot."""
    module = controller.return_value.get_module.return_value
    module.get_name.return_value = "Kitchen switch"
    module.get_type_name.return_value = "VMBGP4"
    module.get_address.return_value = 5
    module.calc_channel_offset.return_value = 0
    module.get_channels.return_value = {}
    return module


@pytest.mark.usefixtures("source_module")
async def test_all_actions_names_both_ends_of_every_slot(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """The flat list has to say what triggers what, in both directions."""
    await init_integration(hass, config_entry)
    client = await hass_ws_client(hass)

    with patch(
        "homeassistant.components.velbus.websocket_api.cached_actions",
        AsyncMock(return_value=_scan()),
    ):
        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/all",
                "config_entry": config_entry.entry_id,
            }
        )
        response = await client.receive_json()

    assert response["success"]
    action = response["result"]["actions"][0]
    assert (action["address"], action["channel"]) == (1, 2)
    assert action["source_module_address"] == 5
    assert action["source_module_name"] == "Kitchen switch"
    assert response["result"]["modules"][0]["from_cache"] is True


@pytest.mark.usefixtures("source_module")
async def test_scan_reports_progress_then_the_result(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """A read of minutes has to say how far it is while it runs."""
    await init_integration(hass, config_entry)
    client = await hass_ws_client(hass)

    async def fake_scan(controller, *, force, addresses, progress):
        progress(ScanProgress(done=0, total=1, address=1, name="Relay"))
        return _scan()

    save = AsyncMock(return_value=[1])
    with (
        patch("homeassistant.components.velbus.websocket_api.scan_actions", fake_scan),
        patch("homeassistant.components.velbus.websocket_api.save_action_cache", save),
    ):
        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/scan",
                "config_entry": config_entry.entry_id,
                "force": True,
            }
        )
        assert (await client.receive_json())["success"]
        progress = await client.receive_json()
        done = await client.receive_json()

    assert progress["event"] == {
        "type": "progress",
        "done": 0,
        "total": 1,
        "address": 1,
        "name": "Relay",
    }
    assert done["event"]["type"] == "done"
    assert done["event"]["action_count"] == 1
    assert save.await_args.kwargs["addresses"] == [1]


@pytest.mark.usefixtures("source_module")
async def test_scan_does_not_write_a_module_it_could_not_read(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """A failed read must not replace what an earlier scan learned."""
    await init_integration(hass, config_entry)
    client = await hass_ws_client(hass)

    save = AsyncMock(return_value=[])
    with (
        patch(
            "homeassistant.components.velbus.websocket_api.scan_actions",
            AsyncMock(return_value=_scan(error="timeout")),
        ),
        patch("homeassistant.components.velbus.websocket_api.save_action_cache", save),
    ):
        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/scan",
                "config_entry": config_entry.entry_id,
            }
        )
        assert (await client.receive_json())["success"]
        await client.receive_json()

    assert save.await_args.kwargs["addresses"] == []


async def test_second_scan_is_refused_while_one_runs(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """Two scans on one bus would interleave their memory requests."""
    await init_integration(hass, config_entry)
    client = await hass_ws_client(hass)
    started = asyncio.Event()

    async def never_finishes(controller, *, force, addresses, progress):
        started.set()
        await asyncio.Event().wait()

    with patch(
        "homeassistant.components.velbus.websocket_api.scan_actions", never_finishes
    ):
        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/scan",
                "config_entry": config_entry.entry_id,
            }
        )
        assert (await client.receive_json())["success"]
        await started.wait()

        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/scan",
                "config_entry": config_entry.entry_id,
            }
        )
        response = await client.receive_json()

    assert response["success"] is False
    assert "already running" in response["error"]["message"]


async def test_clear_cache_reports_what_it_forgot(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    config_entry: MockConfigEntry,
) -> None:
    """Clearing has to say which modules will be read again."""
    await init_integration(hass, config_entry)
    client = await hass_ws_client(hass)

    with patch(
        "homeassistant.components.velbus.websocket_api.clear_action_cache",
        AsyncMock(return_value=[1, 2]),
    ):
        await client.send_json_auto_id(
            {
                "type": "velbus/config_panel/actions/clear_cache",
                "config_entry": config_entry.entry_id,
            }
        )
        response = await client.receive_json()

    assert response["success"]
    assert response["result"] == {"cleared": [1, 2]}
