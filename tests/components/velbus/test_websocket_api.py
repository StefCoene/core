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
