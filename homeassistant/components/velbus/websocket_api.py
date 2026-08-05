"""Velbus config panel websocket API."""

import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, overload

import velbus_frontend as velbus_panel
from velbusaio.action_cache import (
    ActionScan,
    ScanProgress,
    cached_actions,
    clear_action_cache,
    save_action_cache,
    scan_actions,
)
from velbusaio.autosend import decode_autosend_interval
from velbusaio.exceptions import VelbusConfigError
from velbusaio.panel_schema import get_module_instance_data, get_module_type_schema
import voluptuous as vol

from homeassistant.components import frontend, panel_custom, websocket_api
from homeassistant.components.frontend import async_panel_exists
from homeassistant.components.http import StaticPathConfig
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import device_registry as dr
from homeassistant.util import dt as dt_util
from homeassistant.util.hass_dict import HassKey

from .config import is_advanced_mode_enabled, require_advanced_mode
from .const import CONF_CHANNEL, CONF_CONFIG_ENTRY, DOMAIN
from .data import VelbusConfigEntry

if TYPE_CHECKING:
    from velbusaio.channels import Channel
    from velbusaio.config import ConfigParameter
    from velbusaio.controller import Velbus
    from velbusaio.module import Module

_LOGGER = logging.getLogger(__name__)

URL_BASE: Final = "/velbus_static"
DATA_STATIC_REGISTERED: Final = "static_registered"
DATA_WS_REGISTERED: Final = "ws_registered"
DATA_PANEL: HassKey[dict[str, bool]] = HassKey(f"{DOMAIN}_panel")
DATA_ACTION_SCAN: HassKey[dict[str, asyncio.Task[None] | None]] = HassKey(
    f"{DOMAIN}_action_scan"
)

type VelbusWebSocketHandler = Callable[
    [
        HomeAssistant,
        VelbusConfigEntry,
        Velbus,
        websocket_api.ActiveConnection,
        dict[str, Any],
    ],
    None,
]
type VelbusAsyncWebSocketHandler = Callable[
    [
        HomeAssistant,
        VelbusConfigEntry,
        Velbus,
        websocket_api.ActiveConnection,
        dict[str, Any],
    ],
    Awaitable[None],
]


@callback
def async_register_websocket_api(hass: HomeAssistant) -> None:
    """Register Velbus websocket commands once."""
    panel_data = hass.data.setdefault(DATA_PANEL, {})
    if panel_data.get(DATA_WS_REGISTERED):
        return

    websocket_api.async_register_command(hass, ws_get_base_data)
    websocket_api.async_register_command(hass, ws_list_modules)
    websocket_api.async_register_command(hass, ws_get_module_schema)
    websocket_api.async_register_command(hass, ws_get_module)
    websocket_api.async_register_command(hass, ws_set_module_config)
    websocket_api.async_register_command(hass, ws_list_shared_config)
    websocket_api.async_register_command(hass, ws_set_shared_config)
    websocket_api.async_register_command(hass, ws_sync_clock)
    websocket_api.async_register_command(hass, ws_get_all_actions)
    websocket_api.async_register_command(hass, ws_scan_actions)
    websocket_api.async_register_command(hass, ws_clear_action_cache)
    websocket_api.async_register_command(hass, ws_get_channel_actions)
    websocket_api.async_register_command(hass, ws_set_channel_action)
    websocket_api.async_register_command(hass, ws_clear_channel_action)
    panel_data[DATA_WS_REGISTERED] = True


def _any_advanced_mode_enabled(
    hass: HomeAssistant, *, unloading_entry_id: str | None = None
) -> bool:
    """Return whether any Velbus entry has advanced mode enabled."""
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.entry_id == unloading_entry_id:
            continue
        # Include SETUP_IN_PROGRESS so the panel can register during setup,
        # before the entry state becomes LOADED.
        if entry.state not in (
            ConfigEntryState.LOADED,
            ConfigEntryState.SETUP_IN_PROGRESS,
        ):
            continue
        if is_advanced_mode_enabled(entry):
            return True
    return False


def _panel_url_version() -> str:
    """Return the path segment identifying this build of the panel.

    A development checkout keeps the same version while its files change, and
    serves them without cache headers, which still lets a browser reuse what it
    has for a heuristic freshness window. Folding the newest modification time
    into the path gives every restart its own url.
    """
    if velbus_panel.is_prod_build:
        return str(velbus_panel.__version__)
    newest = max(
        (
            path.stat().st_mtime_ns
            for path in Path(velbus_panel.locate_dir()).rglob("*")
            if path.is_file()
        ),
        default=0,
    )
    return f"{velbus_panel.__version__}-{newest:x}"


async def async_update_panel(
    hass: HomeAssistant, *, unloading_entry_id: str | None = None
) -> None:
    """Register or remove the Velbus config panel based on advanced mode."""
    if not _any_advanced_mode_enabled(hass, unloading_entry_id=unloading_entry_id):
        frontend.async_remove_panel(hass, DOMAIN, warn_if_unknown=False)
        return

    # The version goes in the path rather than a query on the entry point. The
    # panel is a set of ES modules that import each other by relative path, so
    # only a versioned directory gives every one of them a new URL on an
    # upgrade; a query on the entry point alone leaves the rest cached.
    url_base = f"{URL_BASE}/{await hass.async_add_executor_job(_panel_url_version)}"
    panel_data = hass.data.setdefault(DATA_PANEL, {})
    if not panel_data.get(DATA_STATIC_REGISTERED):
        await hass.http.async_register_static_paths(
            [
                StaticPathConfig(
                    url_base,
                    path=velbus_panel.locate_dir(),
                    cache_headers=velbus_panel.is_prod_build,
                )
            ]
        )
        panel_data[DATA_STATIC_REGISTERED] = True

    if async_panel_exists(hass, DOMAIN):
        return

    await panel_custom.async_register_panel(
        hass=hass,
        frontend_url_path=DOMAIN,
        config_panel_domain=DOMAIN,
        webcomponent_name=velbus_panel.webcomponent_name,
        module_url=f"{url_base}/{velbus_panel.entrypoint_js}",
        embed_iframe=True,
        require_admin=True,
    )


def _get_entry(
    hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict[str, Any]
) -> VelbusConfigEntry | None:
    entry_id = msg.get(CONF_CONFIG_ENTRY)
    if entry_id is None:
        connection.send_error(
            msg["id"], websocket_api.const.ERR_INVALID_FORMAT, "Missing config_entry"
        )
        return None
    entry = hass.config_entries.async_get_entry(entry_id)
    if entry is None or entry.domain != DOMAIN:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_NOT_FOUND,
            f"Config entry '{entry_id}' not found",
        )
        return None
    if entry.state is not ConfigEntryState.LOADED:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            "Velbus config entry is not loaded",
        )
        return None
    return entry


@overload
def provide_velbus(
    func: VelbusAsyncWebSocketHandler,
) -> websocket_api.const.AsyncWebSocketCommandHandler: ...
@overload
def provide_velbus(
    func: VelbusWebSocketHandler,
) -> websocket_api.const.WebSocketCommandHandler: ...


def provide_velbus(
    func: VelbusAsyncWebSocketHandler | VelbusWebSocketHandler,
) -> (
    websocket_api.const.AsyncWebSocketCommandHandler
    | websocket_api.const.WebSocketCommandHandler
):
    """Websocket decorator to provide a Velbus config entry and controller."""

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def with_velbus(
            hass: HomeAssistant,
            connection: websocket_api.ActiveConnection,
            msg: dict[str, Any],
        ) -> None:
            entry = _get_entry(hass, connection, msg)
            if entry is None:
                return
            await func(hass, entry, entry.runtime_data.controller, connection, msg)

    else:

        @wraps(func)
        def with_velbus(
            hass: HomeAssistant,
            connection: websocket_api.ActiveConnection,
            msg: dict[str, Any],
        ) -> None:
            entry = _get_entry(hass, connection, msg)
            if entry is None:
                return
            func(hass, entry, entry.runtime_data.controller, connection, msg)

    return with_velbus


def _device_id_for_module(
    hass: HomeAssistant, entry: VelbusConfigEntry, address: int
) -> str | None:
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get_device(identifiers={(DOMAIN, str(address))})
    if device is None or entry.entry_id not in device.config_entries:
        return None
    return device.id


def _name_source(controller: Velbus, slot: dict[str, Any]) -> dict[str, Any]:
    """Name the module and channel an action slot points at.

    A slot stores a bare address and channel number. The address may be a
    subaddress, whose channels are numbered from one again, so the module has
    to translate it back before the name means anything.
    """
    address = slot.get("source_address")
    module = controller.get_module(address) if address else None
    if module is None:
        return slot

    slot["source_module_name"] = module.get_name()
    slot["source_module_type"] = module.get_type_name()
    # The address the panel needs to pick the module out of its own list, which
    # is keyed on the primary address even when the slot points at a subaddress.
    slot["source_module_address"] = module.get_address()
    source_channel = slot.get("source_channel")
    if source_channel is not None:
        offset = module.calc_channel_offset(address)
        slot["source_module_channel"] = source_channel + offset
        channel = module.get_channels().get(source_channel + offset)
        if channel is not None:
            slot["source_channel_name"] = channel.get_name()
    return slot


def _bus_location(module: Module, channel: int) -> tuple[int, int]:
    """Return the address and channel the bus uses for a module channel.

    A module numbers its channels through, but those above eight live on a
    subaddress and are numbered from one again there. An action table stores
    what the bus uses, so that is what has to be written into it.
    """
    block, offset = divmod(channel - 1, 8)
    address = module.get_sub_address_dict().get(block)
    if block == 0 or address is None:
        return module.get_address(), channel
    return address, offset + 1


def _get_relay_channel(controller: Velbus, address: int, channel: int) -> Channel:
    module = controller.get_module(address)
    if module is None:
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="module_not_found",
            translation_placeholders={"address": str(address)},
        )
    relay = module.get_channels().get(channel)
    if relay is None or not hasattr(relay, "get_action_table"):
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="relay_channel_not_found",
            translation_placeholders={
                "address": str(address),
                "channel": str(channel),
            },
        )
    return relay


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/get_base_data",
        vol.Required(CONF_CONFIG_ENTRY): str,
    }
)
@provide_velbus
@callback
def ws_get_base_data(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return base panel data."""
    connection.send_result(
        msg["id"],
        {
            "config_entry_id": entry.entry_id,
            "advanced_mode": is_advanced_mode_enabled(entry),
            "title": entry.title,
        },
    )


def _as_autosend(state: tuple[str, int | None]) -> dict[str, Any]:
    """Return an auto send (mode, seconds) pair as a json friendly mapping."""
    mode, seconds = state
    return {"mode": mode, "seconds": seconds}


def _autosend_state(module: Module) -> dict[str, Any] | None:
    """Return how often the module sends its temperature, if it is known."""
    if not module.supports_temperature():
        return None
    interval = module.get_temp_autosend_interval()
    if interval is None:
        return {"mode": "unknown", "seconds": None}
    return _as_autosend(decode_autosend_interval(interval))


async def _read_temp_settings(controller: Velbus) -> None:
    """Read the temperature settings of every module that has them.

    Unlike the light value interval, which rides along with a module status
    message, these are only ever sent in reply to a request. ensure_loaded()
    asks at most once per module, and a module that stays silent is left
    unknown rather than failing the whole list.

    The interval lives in settings Part2, so a module that does not send that
    part cannot answer this question and is not asked; the PIR modules keep the
    value in eeprom and are read from there instead. Asking anyway would cost a
    timeout per module on every call.
    """
    requests: list[Awaitable[None]] = [
        module.refresh_autosend_intervals()
        for module in controller.get_modules().values()
        if module.get_autosend_kinds()
    ]
    requests += [
        settings.ensure_loaded()
        for module in controller.get_modules().values()
        if (settings := module.get_temp_settings()) is not None and settings.has_part2
    ]
    if requests:
        await asyncio.gather(*requests, return_exceptions=True)


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/modules",
        vol.Required(CONF_CONFIG_ENTRY): str,
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_list_modules(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """List modules for the config panel."""
    await _read_temp_settings(controller)
    modules = []
    for module in controller.get_modules().values():
        address = module.get_addresses()[0]
        temp_autosend = _autosend_state(module)
        light = module.get_properties().get("light_value")
        channels = {}
        for channel_num, channel in module.get_channels().items():
            bus_address, bus_channel = _bus_location(module, channel_num)
            channels[str(channel_num)] = {
                "name": channel.get_name(),
                "bus_address": bus_address,
                "bus_channel": bus_channel,
            }
        modules.append(
            {
                "address": address,
                "name": module.get_name(),
                "type_id": module.get_type(),
                "type_name": module.get_type_name(),
                "serial": module.get_serial(),
                "firmware_build": module.get_sw_version(),
                "memory_map_build": module.get_memory_map_build(),
                "memory_map_outdated": module.is_memory_map_outdated(),
                "temp_autosend": temp_autosend,
                "light_autosend": _as_autosend(light.get_autosend()) if light else None,
                "device_id": _device_id_for_module(hass, entry, address),
                "channels": channels,
            }
        )
    modules.sort(key=lambda item: item["address"])
    connection.send_result(msg["id"], {"modules": modules})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/schema",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required("type_id"): vol.All(vol.Coerce(int), vol.Range(min=0, max=255)),
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_get_module_schema(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return the schema for a module type."""
    schema = await hass.async_add_executor_job(get_module_type_schema, msg["type_id"])
    connection.send_result(msg["id"], schema)


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/get",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required(CONF_ADDRESS): vol.All(vol.Coerce(int), vol.Range(min=1, max=254)),
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_get_module(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return live data for one module."""
    module = controller.get_module(msg[CONF_ADDRESS])
    if module is None:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_NOT_FOUND,
            f"Module {msg[CONF_ADDRESS]} not found",
        )
        return
    data = await get_module_instance_data(module)
    data["device_id"] = _device_id_for_module(hass, entry, msg[CONF_ADDRESS])
    data["schema"] = await hass.async_add_executor_job(
        get_module_type_schema, module.get_type()
    )
    connection.send_result(msg["id"], data)


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/config/set",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required(CONF_ADDRESS): vol.All(vol.Coerce(int), vol.Range(min=1, max=254)),
        # Specs include editable channels above 32 (e.g. temperature name 33/34).
        # Channel 0 is a module level setting, which lives on a property.
        vol.Required(CONF_CHANNEL): vol.All(vol.Coerce(int), vol.Range(min=0, max=64)),
        vol.Required("key"): str,
        vol.Required("value"): vol.Any(str, bool, int, float),
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_set_module_config(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Write a module or channel configuration parameter."""
    module = controller.get_module(msg[CONF_ADDRESS])
    if module is None:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_NOT_FOUND,
            f"Module {msg[CONF_ADDRESS]} not found",
        )
        return

    param = module.find_config_parameter(msg["key"], msg[CONF_CHANNEL])
    if param is None:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_INVALID_FORMAT,
            f"Unknown config key '{msg['key']}' on channel {msg[CONF_CHANNEL]}",
        )
        return

    # Advanced mode guards the settings that change module memory, where a
    # wrong address corrupts a name or an action table. A setting that only
    # puts a message on the bus is either understood or ignored, so it does
    # not need the same protection.
    if param.writes_memory:
        require_advanced_mode(entry)

    try:
        await param.set_value(msg["value"])
    except (OSError, RuntimeError, ValueError, VelbusConfigError) as err:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            str(err),
        )
        return

    connection.send_result(msg["id"], {"success": True})


def _shared_parameters(
    controller: Velbus,
) -> dict[str, list[tuple[Module, ConfigParameter]]]:
    """Group the settings that more than one module has in common.

    What counts as module wide is how often a module has the setting, not which
    channel it is filed under: Inhibit is there once per relay and means
    something different on each, while a temperature autosend interval is there
    once even though it hangs off the temperature channel.

    Settings that write eeprom are left out: those are the ones where a wrong
    address corrupts a module, and doing that to a whole installation in one
    click is not a button worth having.
    """
    shared: dict[str, list[tuple[Module, ConfigParameter]]] = {}
    for module in controller.get_modules().values():
        per_module: dict[str, list[ConfigParameter]] = {}
        for param in module.get_config_parameters():
            if param.writes_memory:
                continue
            per_module.setdefault(param.key, []).append(param)
        for key, params in per_module.items():
            if len(params) == 1:
                shared.setdefault(key, []).append((module, params[0]))
    return {key: entries for key, entries in shared.items() if len(entries) > 1}


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/config/shared",
        vol.Required(CONF_CONFIG_ENTRY): str,
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_list_shared_config(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """List the settings that can be written to every module at once."""
    await _read_temp_settings(controller)
    shared = _shared_parameters(controller)
    values = await asyncio.gather(
        *(param.get_value() for entries in shared.values() for _, param in entries),
        return_exceptions=True,
    )
    read = iter(values)

    settings = []
    for entries in shared.values():
        first = entries[0][1]
        modules = []
        for module, _param in entries:
            value = next(read)
            modules.append(
                {
                    "address": module.get_addresses()[0],
                    "name": module.get_name(),
                    "value": None if isinstance(value, BaseException) else value,
                }
            )
        settings.append(
            {
                **first.to_dict(),
                "modules": sorted(modules, key=lambda item: item["address"]),
            }
        )
    connection.send_result(msg["id"], {"settings": settings})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/config/set_shared",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required("key"): str,
        vol.Required("value"): vol.Any(str, bool, int, float),
        # Which modules to write. Absent means every module that has the
        # setting; the panel sends the list it showed, so a module that
        # appeared after the page was drawn is not written unnoticed.
        vol.Optional("addresses"): [
            vol.All(vol.Coerce(int), vol.Range(min=1, max=254))
        ],
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_set_shared_config(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Write one setting to every module that has it."""
    entries = _shared_parameters(controller).get(msg["key"])
    if not entries:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_INVALID_FORMAT,
            f"No module shares the setting '{msg['key']}'",
        )
        return

    wanted = msg.get("addresses")
    if wanted is not None:
        entries = [
            entry_pair
            for entry_pair in entries
            if entry_pair[0].get_addresses()[0] in wanted
        ]

    # One module that does not answer must not hide what happened to the rest,
    # so every write is reported separately instead of failing the whole call.
    outcomes = await asyncio.gather(
        *(param.set_value(msg["value"]) for _module, param in entries),
        return_exceptions=True,
    )
    results = [
        {
            "address": module.get_addresses()[0],
            "name": module.get_name(),
            "success": not isinstance(outcome, BaseException),
            "error": str(outcome) if isinstance(outcome, BaseException) else None,
        }
        for (module, _param), outcome in zip(entries, outcomes, strict=True)
    ]
    connection.send_result(msg["id"], {"results": results})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/sync_clock",
        vol.Required(CONF_CONFIG_ENTRY): str,
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_sync_clock(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Broadcast the current time to every module."""
    try:
        # Home Assistant's own local time, not the clock of whatever host holds
        # the bus connection.
        await controller.sync_clock(dt_util.now())
    except OSError as err:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            str(err),
        )
        return

    connection.send_result(msg["id"], {"success": True})


async def _persist_actions(controller: Velbus, address: int) -> None:
    """Keep the cached copy of one module in step with what was just written.

    Writing updates the in-memory bytes; without this the file would keep
    describing the slot as it was before, which is worse than having no file at
    all. A module that was never fully read writes nothing, so this is a no-op
    until somebody has scanned it.
    """
    try:
        await save_action_cache(controller, addresses=[address])
    except OSError as err:
        _LOGGER.warning("Could not update the cached actions of %s: %s", address, err)


def _scan_payload(controller: Velbus, scan: ActionScan) -> dict[str, Any]:
    """Turn a scan into what the panel needs to draw both directions.

    The actions come out as one flat list rather than nested per module. Every
    entry names both ends, so the same list indexes as "what triggers this
    channel" and as "what does this button do", which is the whole reason for
    reading the installation in one go.
    """
    modules: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    for module_actions in scan.modules.values():
        modules.append(
            {
                "address": module_actions.address,
                "name": module_actions.name,
                "type_name": module_actions.type_name,
                "read_at": module_actions.read_at,
                "from_cache": module_actions.from_cache,
                "action_count": module_actions.action_count,
                "error": module_actions.error,
            }
        )
        actions.extend(
            {
                **_name_source(controller, slot.to_dict()),
                "address": module_actions.address,
                "channel": channel,
            }
            for channel, slots in module_actions.channels.items()
            for slot in slots
        )
    return {
        "modules": modules,
        "actions": actions,
        "action_count": scan.action_count,
        "duration": scan.duration,
    }


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/actions/all",
        vol.Required(CONF_CONFIG_ENTRY): str,
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_get_all_actions(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return every action table that is already known, without reading the bus."""
    connection.send_result(
        msg["id"], _scan_payload(controller, await cached_actions(controller))
    )


async def _run_action_scan(
    hass: HomeAssistant,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Read the installation and report on it as it goes."""

    @callback
    def report(step: ScanProgress) -> None:
        connection.send_message(
            websocket_api.event_message(
                msg["id"],
                {
                    "type": "progress",
                    "done": step.done,
                    "total": step.total,
                    "address": step.address,
                    "name": step.name,
                },
            )
        )

    try:
        scan = await scan_actions(
            controller,
            force=msg["force"],
            addresses=msg.get("addresses"),
            progress=report,
        )
        # Only what was read is written, so a module that timed out keeps
        # whatever an earlier scan learned about it.
        await save_action_cache(
            controller,
            addresses=[
                address
                for address, module in scan.modules.items()
                if module.error is None
            ],
        )
    except (OSError, RuntimeError, ValueError, VelbusConfigError) as err:
        connection.send_message(
            websocket_api.event_message(
                msg["id"], {"type": "error", "message": str(err)}
            )
        )
        return
    finally:
        hass.data.setdefault(DATA_ACTION_SCAN, {}).pop(msg[CONF_CONFIG_ENTRY], None)

    connection.send_message(
        websocket_api.event_message(
            msg["id"], {"type": "done", **_scan_payload(controller, scan)}
        )
    )


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/actions/scan",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Optional("force", default=False): bool,
        vol.Optional("addresses"): [
            vol.All(vol.Coerce(int), vol.Range(min=1, max=254))
        ],
    }
)
@provide_velbus
@callback
def ws_scan_actions(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Read every action table, reporting progress as events.

    A full read is minutes of bus traffic, far longer than a page is willing to
    wait for one answer, so this subscribes: progress events while it runs, one
    done event at the end. Closing the subscription cancels the scan.
    """
    running = hass.data.setdefault(DATA_ACTION_SCAN, {})
    if msg[CONF_CONFIG_ENTRY] in running:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            "A scan of this bus is already running",
        )
        return

    task: asyncio.Task[None] | None = None

    @callback
    def cancel_scan() -> None:
        if task is not None:
            task.cancel()

    # Subscribe first: Home Assistant starts a task eagerly, so a scan that
    # reports progress before its first await would send that event ahead of
    # the answer to this very command.
    connection.subscriptions[msg["id"]] = cancel_scan
    connection.send_result(msg["id"])

    # Claim the bus before the task exists, for the same reason: an eagerly
    # started scan can already have finished and cleared this entry by the time
    # async_create_task() returns, and writing the task in afterwards would
    # leave a finished scan blocking every next one.
    running[msg[CONF_CONFIG_ENTRY]] = None
    task = hass.async_create_task(
        _run_action_scan(hass, controller, connection, msg),
        f"velbus action scan {msg[CONF_CONFIG_ENTRY]}",
    )
    if msg[CONF_CONFIG_ENTRY] in running:
        running[msg[CONF_CONFIG_ENTRY]] = task


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/actions/clear_cache",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Optional("addresses"): [
            vol.All(vol.Coerce(int), vol.Range(min=1, max=254))
        ],
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_clear_action_cache(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Forget the cached action tables so the next scan reads the bus again."""
    try:
        cleared = await clear_action_cache(controller, addresses=msg.get("addresses"))
    except OSError as err:
        connection.send_error(
            msg["id"], websocket_api.const.ERR_HOME_ASSISTANT_ERROR, str(err)
        )
        return
    connection.send_result(msg["id"], {"cleared": cleared})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/actions/get",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required(CONF_ADDRESS): vol.All(vol.Coerce(int), vol.Range(min=1, max=254)),
        vol.Required(CONF_CHANNEL): vol.All(vol.Coerce(int), vol.Range(min=1, max=32)),
        vol.Optional("refresh", default=False): bool,
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_get_channel_actions(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Read the action table for a relay channel."""
    try:
        relay = _get_relay_channel(controller, msg[CONF_ADDRESS], msg[CONF_CHANNEL])
    except ServiceValidationError as err:
        connection.send_error(msg["id"], websocket_api.const.ERR_NOT_FOUND, str(err))
        return

    table = relay.get_action_table()
    if table is None:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            "This relay does not support action-table programming",
        )
        return

    slots = await table.get_actions(refresh=msg["refresh"], include_empty=True)
    connection.send_result(
        msg["id"],
        {"slots": [_name_source(controller, slot.to_dict()) for slot in slots]},
    )


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/actions/set",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required(CONF_ADDRESS): vol.All(vol.Coerce(int), vol.Range(min=1, max=254)),
        vol.Required(CONF_CHANNEL): vol.All(vol.Coerce(int), vol.Range(min=1, max=32)),
        vol.Required("source_address"): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=254)
        ),
        vol.Required("action"): vol.Any(str, int),
        vol.Optional("source_channel"): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=32)
        ),
        vol.Optional("slot"): vol.All(vol.Coerce(int), vol.Range(min=0, max=255)),
        vol.Optional("time1", default=0xFF): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
        vol.Optional("time2", default=0xFF): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
        vol.Optional("time3", default=0xFF): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_set_channel_action(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Program an action slot on a relay channel."""
    require_advanced_mode(entry)
    try:
        relay = _get_relay_channel(controller, msg[CONF_ADDRESS], msg[CONF_CHANNEL])
    except ServiceValidationError as err:
        connection.send_error(msg["id"], websocket_api.const.ERR_NOT_FOUND, str(err))
        return

    # A channel that triggers itself feeds its own output back into its input.
    module = controller.get_module(msg[CONF_ADDRESS])
    if module is not None and (msg["source_address"], msg.get("source_channel")) == (
        _bus_location(module, msg[CONF_CHANNEL])
    ):
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_INVALID_FORMAT,
            "A channel cannot be its own action source",
        )
        return

    try:
        slot = await relay.set_action(
            source_address=msg["source_address"],
            action=msg["action"],
            source_channel=msg.get("source_channel"),
            slot=msg.get("slot"),
            time1=msg["time1"],
            time2=msg["time2"],
            time3=msg["time3"],
        )
    except (OSError, RuntimeError, ValueError, VelbusConfigError) as err:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            str(err),
        )
        return

    await _persist_actions(controller, msg[CONF_ADDRESS])
    connection.send_result(msg["id"], {"slot": slot.to_dict()})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "velbus/config_panel/module/actions/clear",
        vol.Required(CONF_CONFIG_ENTRY): str,
        vol.Required(CONF_ADDRESS): vol.All(vol.Coerce(int), vol.Range(min=1, max=254)),
        vol.Required(CONF_CHANNEL): vol.All(vol.Coerce(int), vol.Range(min=1, max=32)),
        vol.Optional("slot"): vol.All(vol.Coerce(int), vol.Range(min=0, max=255)),
        vol.Optional("source_address"): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=254)
        ),
        vol.Optional("source_channel"): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=32)
        ),
    }
)
@websocket_api.async_response
@provide_velbus
async def ws_clear_channel_action(
    hass: HomeAssistant,
    entry: VelbusConfigEntry,
    controller: Velbus,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Clear one or more action slots on a relay channel."""
    require_advanced_mode(entry)
    try:
        relay = _get_relay_channel(controller, msg[CONF_ADDRESS], msg[CONF_CHANNEL])
    except ServiceValidationError as err:
        connection.send_error(msg["id"], websocket_api.const.ERR_NOT_FOUND, str(err))
        return

    try:
        if msg.get("slot") is not None:
            cleared = [await relay.clear_action(msg["slot"])]
        elif msg.get("source_address") is not None:
            cleared = await relay.clear_actions_for_source(
                msg["source_address"],
                source_channel=msg.get("source_channel"),
            )
        else:
            connection.send_error(
                msg["id"],
                websocket_api.const.ERR_INVALID_FORMAT,
                "Provide slot or source_address",
            )
            return
    except (OSError, RuntimeError, ValueError, VelbusConfigError) as err:
        connection.send_error(
            msg["id"],
            websocket_api.const.ERR_HOME_ASSISTANT_ERROR,
            str(err),
        )
        return

    await _persist_actions(controller, msg[CONF_ADDRESS])
    connection.send_result(
        msg["id"],
        {"slots": [slot.to_dict() for slot in cleared]},
    )
