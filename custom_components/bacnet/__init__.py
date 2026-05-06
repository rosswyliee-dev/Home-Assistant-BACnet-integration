"""
BACnet IP Integration for Home Assistant.

This integration provides full BACnet/IP support including:
- Local network and BBMD / Foreign Device Registration for cross-subnet communication
- Automatic device discovery via Who-Is / I-Am
- Per-object COV subscriptions with automatic polling fallback
- Read/write with proper Priority Array handling
- Dynamic domain mapping (sensor, switch, number, binary_sensor, climate)

All configuration is done via the GUI (config_flow / options_flow).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    CONF_BBMD_ADDRESS,
    CONF_BBMD_TTL,
    CONF_COV_INCREMENT,
    CONF_DOMAIN_MAPPING,
    CONF_ENABLE_COV,
    CONF_FIRMWARE_VERSION,
    CONF_LOCAL_IP,
    CONF_LOCAL_PORT,
    CONF_MODEL_NAME,
    CONF_POLLING_INTERVAL,
    CONF_SELECTED_OBJECTS,
    CONF_SOFTWARE_VERSION,
    CONF_USE_BBMD,
    CONF_USE_DESCRIPTION,
    CONF_VENDOR_NAME,
    DATA_CLIENT,
    DATA_COORDINATOR,
    DATA_DEVICE_INFO,
    DATA_OBJECTS,
    DATA_UNSUB,
    DEFAULT_COV_INCREMENT,
    DEFAULT_DOMAIN_MAP,
    DEFAULT_ENABLE_COV,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_USE_DESCRIPTION,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

# All platforms that this integration can dynamically register entities on.
PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SWITCH,
    Platform.NUMBER,
    Platform.CLIMATE,
]

# Key prefix for the shared BACnet client stored in hass.data[DOMAIN].
# One client per local port — all config entries on the same port share it.
_SHARED_CLIENT_PREFIX = "shared_client_"

# Key for the asyncio.Lock that serialises shared-client creation/teardown.
# Without this, parallel async_setup_entry calls during HA startup could
# both pass the "not in hass.data" check and each try to bind the UDP port.
_SETUP_LOCK_KEY = "setup_lock"


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _shared_client_key(local_port: int) -> str:
    """Return the hass.data key for the shared BACnet client on *local_port*."""
    return f"{_SHARED_CLIENT_PREFIX}{local_port}"


def _get_platforms_in_use(
    objects: list[dict], domain_overrides: dict[str, str]
) -> list[Platform]:
    """Determine which HA platforms are actually needed based on selected objects.

    This avoids setting up platform files that have zero entities, which
    keeps startup quick and log output clean.
    """
    domains_needed: set[str] = set()
    for obj in objects:
        obj_key = f"{obj['object_type']}:{obj['instance']}"
        domain = domain_overrides.get(
            obj_key, DEFAULT_DOMAIN_MAP.get(obj["object_type"], "sensor")
        )
        domains_needed.add(domain)
    return [Platform(d) for d in domains_needed if d in {p.value for p in PLATFORMS}]


def _count_active_entries(hass: HomeAssistant) -> int:
    """Count config entries currently stored in hass.data[DOMAIN].

    Excludes internal keys (shared clients, setup lock) so the count
    reflects only real config entries.
    """
    return sum(
        1
        for k in hass.data.get(DOMAIN, {})
        if not str(k).startswith(_SHARED_CLIENT_PREFIX)
        and k != _SETUP_LOCK_KEY
    )


# ---------------------------------------------------------------------------
# Integration lifecycle
# ---------------------------------------------------------------------------


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up BACnet IP from a config entry.

    This is called by Home Assistant when the user completes the config flow
    or when HA starts and an existing config entry is loaded.

    Lifecycle:
    1. Acquire the setup lock to prevent race conditions during parallel startup.
    2. Get or create the shared BACnetClient for this UDP port.
    3. Build the per-entry data coordinator for COV + polling fallback.
    4. Store runtime references in hass.data so platforms can access them.
    5. Forward setup to the required platform files.
    """
    # Lazy import to avoid loading BACpypes3 at integration discovery time
    from .bacnet_client import BACnetClient  # noqa: WPS433
    from .coordinator import BACnetCoordinator  # noqa: WPS433

    hass.data.setdefault(DOMAIN, {})

    # Ensure a persistent asyncio.Lock exists for safe concurrent entry setup.
    # Multiple entries may be set up in parallel during HA startup — the lock
    # guarantees only one entry creates the shared client at a time.
    if _SETUP_LOCK_KEY not in hass.data[DOMAIN]:
        hass.data[DOMAIN][_SETUP_LOCK_KEY] = asyncio.Lock()

    # ---- 1. Extract configuration ----
    local_ip: str = entry.data.get(CONF_LOCAL_IP, "")
    local_port: int = entry.data.get(CONF_LOCAL_PORT, 47808)
    use_bbmd: bool = entry.data.get(CONF_USE_BBMD, False)
    bbmd_address: str = entry.data.get(CONF_BBMD_ADDRESS, "")
    bbmd_ttl: int = entry.data.get(CONF_BBMD_TTL, 900)
    selected_objects: list[dict[str, Any]] = entry.data.get(CONF_SELECTED_OBJECTS, [])

    # Options (may be updated at runtime via options_flow)
    enable_cov: bool = entry.options.get(CONF_ENABLE_COV, DEFAULT_ENABLE_COV)
    polling_interval: int = entry.options.get(
        CONF_POLLING_INTERVAL, DEFAULT_POLLING_INTERVAL
    )
    use_description: bool = entry.options.get(
        CONF_USE_DESCRIPTION, DEFAULT_USE_DESCRIPTION
    )
    domain_overrides: dict[str, str] = entry.options.get(CONF_DOMAIN_MAPPING, {})
    cov_increment: float = entry.options.get(CONF_COV_INCREMENT, DEFAULT_COV_INCREMENT)

    # ---- 2. Get or create the shared BACnet client ----
    # Only ONE BACpypes3 application can bind UDP port 47808 on this host.
    # All config entries on the same port share a single BACnetClient.
    # The lock prevents two entries from simultaneously passing the
    # "not in hass.data" check and both trying to create a client.
    shared_key = _shared_client_key(local_port)

    async with hass.data[DOMAIN][_SETUP_LOCK_KEY]:
        if shared_key not in hass.data[DOMAIN]:
            _LOGGER.debug(
                "No shared BACnet client found for port %d — creating one", local_port
            )
            client = BACnetClient(
                local_ip=local_ip,
                local_port=local_port,
            )
            try:
                await client.connect(
                    bbmd_address=bbmd_address if use_bbmd else None,
                    bbmd_ttl=bbmd_ttl,
                )
            except Exception as exc:  # noqa: BLE001
                _LOGGER.error(
                    "Failed to start BACnet client on port %d: %s", local_port, exc
                )
                raise ConfigEntryNotReady(
                    f"Cannot connect to BACnet network: {exc}"
                ) from exc

            hass.data[DOMAIN][shared_key] = client
            _LOGGER.info(
                "Shared BACnet client created on port %d for entry '%s'",
                local_port,
                entry.data.get("device_name", entry.entry_id),
            )
        else:
            client = hass.data[DOMAIN][shared_key]
            _LOGGER.info(
                "Reusing shared BACnet client on port %d for entry '%s'",
                local_port,
                entry.data.get("device_name", entry.entry_id),
            )

    # ---- 3. Build coordinator ----
    coordinator = BACnetCoordinator(
        hass=hass,
        client=client,
        objects=selected_objects,
        enable_cov=enable_cov,
        polling_interval=polling_interval,
        use_description=use_description,
        domain_overrides=domain_overrides,
        entry=entry,
        cov_increment=cov_increment,
    )

    # Perform the first data refresh so entities have initial state
    await coordinator.async_config_entry_first_refresh()

    # ---- 4. Store runtime data ----
    hass.data[DOMAIN][entry.entry_id] = {
        DATA_CLIENT: client,
        DATA_COORDINATOR: coordinator,
        DATA_OBJECTS: selected_objects,
        DATA_DEVICE_INFO: {
            "device_id": entry.data.get("device_id"),
            "device_name": entry.data.get("device_name", "BACnet Device"),
            "device_address": entry.data.get("device_address", ""),
            "vendor_name": entry.data.get(CONF_VENDOR_NAME, ""),
            "model_name": entry.data.get(CONF_MODEL_NAME, ""),
            "firmware_version": entry.data.get(CONF_FIRMWARE_VERSION, ""),
            "software_version": entry.data.get(CONF_SOFTWARE_VERSION, ""),
        },
        DATA_UNSUB: [],
    }

    # ---- 5. Forward to platforms ----
    needed_platforms = _get_platforms_in_use(selected_objects, domain_overrides)
    await hass.config_entries.async_forward_entry_setups(entry, needed_platforms)

    # ---- 6. Listen for option changes ----
    unsub = entry.add_update_listener(_async_options_updated)
    hass.data[DOMAIN][entry.entry_id][DATA_UNSUB].append(unsub)

    _LOGGER.info(
        "BACnet integration setup complete for '%s' with %d objects",
        entry.data.get("device_name", "unknown"),
        len(selected_objects),
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a BACnet config entry.

    Called when the user removes the integration or during HA shutdown.
    Cleans up:
    - COV subscriptions (via coordinator shutdown)
    - Polling tasks (via coordinator shutdown)
    - hass.data references for this entry
    - Shared BACnet client (only when the last entry is removed)
    """
    entry_data = hass.data[DOMAIN].get(entry.entry_id)
    if entry_data is None:
        return True

    # Determine which platforms were loaded for this entry
    domain_overrides: dict[str, str] = entry.options.get(CONF_DOMAIN_MAPPING, {})
    selected_objects = entry_data.get(DATA_OBJECTS, [])
    needed_platforms = _get_platforms_in_use(selected_objects, domain_overrides)

    # Unload platforms first
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, needed_platforms
    )

    if unload_ok:
        # Cancel update listener subscriptions
        for unsub in entry_data.get(DATA_UNSUB, []):
            unsub()

        # Shut down coordinator (cancels COV subscriptions & polling tasks)
        coordinator = entry_data.get(DATA_COORDINATOR)
        if coordinator is not None:
            await coordinator.async_shutdown()

        # Remove this entry's runtime data
        hass.data[DOMAIN].pop(entry.entry_id)

        # ---- Shared client teardown ----
        # Only disconnect the shared BACnet client when this is the last
        # entry using it. Other active entries still need the socket open.
        local_port = entry.data.get(CONF_LOCAL_PORT, 47808)
        shared_key = _shared_client_key(local_port)

        # Acquire lock so teardown doesn't race against a concurrent setup
        lock: asyncio.Lock | None = hass.data[DOMAIN].get(_SETUP_LOCK_KEY)
        if lock is not None:
            async with lock:
                await _maybe_disconnect_shared_client(hass, shared_key, local_port)
        else:
            # Lock missing (shouldn't happen) — proceed without it
            await _maybe_disconnect_shared_client(hass, shared_key, local_port)

        _LOGGER.info("BACnet integration unloaded for entry %s", entry.entry_id)

    return unload_ok


async def _maybe_disconnect_shared_client(
    hass: HomeAssistant, shared_key: str, local_port: int
) -> None:
    """Disconnect and remove the shared client if no entries remain.

    Must be called while holding the setup lock.
    """
    remaining = _count_active_entries(hass)

    if remaining == 0:
        client = hass.data[DOMAIN].pop(shared_key, None)
        if client is not None:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                _LOGGER.debug(
                    "Exception disconnecting shared BACnet client on port %d (ignored)",
                    local_port,
                )
            _LOGGER.info(
                "Shared BACnet client on port %d disconnected — no entries remain",
                local_port,
            )
    else:
        _LOGGER.debug(
            "Shared BACnet client on port %d retained — %d entry/entries still active",
            local_port,
            remaining,
        )


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update.

    When the user changes options (COV, polling interval, naming, domain mapping)
    we reload the entire config entry so all entities and the coordinator
    pick up the new settings cleanly.

    The shared client is preserved across the reload because other entries
    remain active — only this entry's coordinator is torn down and rebuilt.
    """
    _LOGGER.debug("Options updated for BACnet entry %s — reloading", entry.entry_id)
    await hass.config_entries.async_reload(entry.entry_id)
