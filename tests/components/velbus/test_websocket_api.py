"""Tests for the Velbus config panel websocket API."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from velbusaio.config import ConfigParameter

from homeassistant.components.velbus.const import CONF_ADVANCED_MODE
from homeassistant.core import HomeAssistant

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
