"""The Ather Electric integration."""

from __future__ import annotations

import asyncio
import logging

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv, entity
from homeassistant.helpers.typing import ConfigType

from .const import (
    CONF_FIREBASE_API_KEY,
    CONF_FIREBASE_TOKEN,
    CONF_SCOOTER_ID,
    DOMAIN,
    PLATFORMS,
    CONF_BASE_URL,
    CONF_ATHER_TOKEN,
    CONF_SCOOTER_UUID,
)
from .coordinator import AtherCoordinator
from .helpers import normalize_api_token

_LOGGER = logging.getLogger(__name__)

# Configuration Schema
CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_SCOOTER_ID): cv.string,
                vol.Required(CONF_FIREBASE_TOKEN): cv.string,
                vol.Optional(CONF_FIREBASE_API_KEY): cv.string,
                vol.Optional(CONF_NAME, default="Ather Scooter"): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Ather Electric integration."""
    conf = config.get(DOMAIN)
    if conf is None:
        return True

    # Ensure API Key is present for import if missing
    if CONF_FIREBASE_API_KEY not in conf:
        _LOGGER.error("YAML configuration is missing required 'firebase_api_key'")
        return False

    hass.async_create_task(
        hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_IMPORT},
            data=conf,
        )
    )

    return True


async def async_migrate_entry(
    hass: HomeAssistant, config_entry: config_entries.ConfigEntry
) -> bool:
    """Normalize legacy token keys during upgrade."""
    data = dict(config_entry.data)
    token = normalize_api_token(data)
    if token:
        data[CONF_ATHER_TOKEN] = token
        data["api_token"] = token

    if data != config_entry.data or config_entry.version < 2:
        hass.config_entries.async_update_entry(
            config_entry,
            data=data,
            version=2,
        )

    return True


from homeassistant.loader import async_get_integration

# ... existing imports ...


async def async_setup_entry(
    hass: HomeAssistant, entry: config_entries.ConfigEntry
) -> bool:
    """Set up Ather Electric from a config entry."""
    scooter_id = entry.data.get(CONF_SCOOTER_ID)
    scooter_uuid = entry.data.get(CONF_SCOOTER_UUID)
    firebase_token = entry.data.get(CONF_FIREBASE_TOKEN, "")
    api_token = normalize_api_token(entry.data) or ""
    api_key = entry.data.get(CONF_FIREBASE_API_KEY, "")
    device_name = entry.data.get(CONF_NAME, "Ather Scooter")
    base_url = entry.data.get(CONF_BASE_URL)

    integration = await async_get_integration(hass, DOMAIN)
    integration_version = integration.version

    # Initialize RideManager
    from .ride_manager import RideManager
    from .const import CONF_RIDE_RETENTION_MONTHS, DEFAULT_RIDE_RETENTION_MONTHS
    from .api import AtherAPI  # Import here
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    ride_api_client = AtherAPI(
        session,
        base_url=base_url if base_url else "https://ather-production.firebaseio.com",
    )

    ride_manager = RideManager(
        hass,
        ride_api_client,
        scooter_id,
        api_token,
        retention_months=entry.options.get(
            CONF_RIDE_RETENTION_MONTHS, DEFAULT_RIDE_RETENTION_MONTHS
        ),
    )

    # Wait for DB Init
    await ride_manager.async_init()

    # Run Initial Sync (limit=100) on startup
    # MOVED to AtherCoordinator.start() using sync_startup()
    # hass.async_create_task(ride_manager.sync_initial())

    coordinator = AtherCoordinator(
        hass,
        scooter_id,
        firebase_token,
        api_token,
        api_key,
        device_name,
        integration_version,
        base_url=base_url,
        ride_manager=ride_manager,  # Pass manager
        config_entry=entry,
    )

    # Start the coordinator (WebSocket connection)
    coordinator.start()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Wait for initial data to ensure feature flags are loaded
    try:
        await asyncio.wait_for(coordinator.async_wait_for_initial_data(), timeout=60)
    except asyncio.TimeoutError:
        _LOGGER.warning(
            "Timed out waiting for initial data. Remote features may not be enabled."
        )
    except Exception as err:
        _LOGGER.error("Error waiting for initial data: %s", err)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Register force sync service
    async def force_sync_service(call: ServiceCall) -> None:
        """Force sync with Ather servers."""
        coordinator = hass.data[DOMAIN][entry.entry_id]
        success = await coordinator.force_sync()

        if success:
            _LOGGER.info("Force sync service completed successfully")
        else:
            _LOGGER.warning("Force sync service completed with issues")

    # Register service with proper schema once per domain.
    if not hass.services.has_service(DOMAIN, "force_sync"):
        hass.services.async_register(
            DOMAIN,
            "force_sync",
            force_sync_service,
            schema=vol.Schema({}),
        )

    # Apply initial options
    if entry.options:
        coordinator.set_options(entry.options)

    # Register update listener
    entry.async_on_unload(entry.add_update_listener(update_listener))

    return True


async def update_listener(hass: HomeAssistant, entry: config_entries.ConfigEntry):
    """Handle options update."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    coordinator.set_options(entry.options)


async def async_unload_entry(
    hass: HomeAssistant, entry: config_entries.ConfigEntry
) -> bool:
    """Unload a config entry."""
    coordinator = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if coordinator:
        await coordinator.close()

    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data.setdefault(DOMAIN, {}).pop(entry.entry_id, None)
        if not hass.config_entries.async_entries(DOMAIN):
            hass.services.async_remove(DOMAIN, "force_sync")

    return unload_ok
