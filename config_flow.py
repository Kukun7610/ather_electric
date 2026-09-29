"""Config flow for Ather Electric."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_NAME
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import AtherAPI
from .const import (
    CONF_FIREBASE_API_KEY,
    CONF_FIREBASE_TOKEN,
    CONF_MOBILE_NO,
    CONF_OTP,
    CONF_SCOOTER_ID,
    DOMAIN,
    CONF_ENABLE_RAW_LOGGING,
    DEFAULT_ENABLE_RAW_LOGGING,
    CONF_BASE_URL,
    CONF_ATHER_TOKEN,
    CONF_SCOOTER_UUID,
    CONF_VIN,
    CONF_MODEL,
)
from homeassistant.core import callback

_LOGGER = logging.getLogger(__name__)


class AtherConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Ather Electric."""

    VERSION = 1

    def __init__(self):
        """Initialize flow."""
        self.mobile_no = None
        self.api_token = None
        self.firebase_token = None
        self.api_key = None
        self.user_id = None
        self.base_url = None
        self.name = "Ather"  # Default name
        self.scooter_ids = []
        self.scooters_list = []

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step (Mobile Number, Name)."""
        errors: dict[str, str] = {}
        if user_input is not None:
            self.mobile_no = user_input[CONF_MOBILE_NO]
            self.api_key = ""
            self.name = user_input.get(CONF_NAME, "Ather")
            session = async_get_clientsession(self.hass)
            api = AtherAPI(session)
            if await api.generate_otp(self.mobile_no):
                return await self.async_step_otp()
            errors["base"] = "otp_generation_failed"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_MOBILE_NO): str,
                    vol.Required(CONF_NAME, default="Ather"): str,
                }
            ),
            errors=errors,
        )

    async def async_step_otp(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle OTP Entry and Auto-Discovery."""
        errors: dict[str, str] = {}
        if user_input is not None:
            otp = user_input[CONF_OTP]
            session = async_get_clientsession(self.hass)
            api = AtherAPI(session)
            tokens = await api.verify_otp(self.mobile_no, otp)

            if tokens:
                self.api_token = tokens.get("token")
                self.firebase_token = tokens.get("firebase_token")

                _LOGGER.debug(
                    "Tokens received. API Token (Len): %s",
                    len(str(self.api_token)) if self.api_token else 0,
                )

                # Fetch Scooters using new endpoint
                try:
                    scooters = await api.get_scooters_v2(self.api_token)
                    if scooters:
                        self.scooters_list = scooters
                        self.scooter_ids = [
                            s.get("scooter_uuid")
                            for s in scooters
                            if s.get("scooter_uuid")
                        ]
                    else:
                        self.scooters_list = []
                        self.scooter_ids = []
                except Exception as e:
                    _LOGGER.error("Failed to fetch scooters list: %s", e)
                    self.scooters_list = []
                    self.scooter_ids = []

                if not self.scooter_ids:
                    errors["base"] = "no_vehicles_found"
                elif len(self.scooter_ids) == 1:
                    # Auto-select
                    return await self.async_create_entry_from_scooter(
                        api, self.scooter_ids[0]
                    )
                else:
                    # Multiple scooters
                    return await self.async_step_select_vehicle()
            else:
                errors["base"] = "invalid_otp"

        return self.async_show_form(
            step_id="otp",
            data_schema=vol.Schema({vol.Required(CONF_OTP): str}),
            errors=errors,
        )

    async def async_step_select_vehicle(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle Vehicle Selection if multiple exist."""
        if user_input is not None:
            session = async_get_clientsession(self.hass)
            api = AtherAPI(session)

            return await self.async_create_entry_from_scooter(
                api, user_input[CONF_SCOOTER_ID]
            )

        return self.async_show_form(
            step_id="select_vehicle",
            data_schema=vol.Schema(
                {vol.Required(CONF_SCOOTER_ID): vol.In(self.scooter_ids)}
            ),
        )

    async def async_create_entry_from_scooter(self, api: AtherAPI, scooter_uuid: str):
        """Create the config entry."""
        # Use the name provided by the user (or default)
        name = self.name

        # Resolve short scooter_id
        scooter_id = scooter_uuid
        for s in self.scooters_list:
            if s.get("scooter_uuid") == scooter_uuid:
                scooter_id = s.get("scooter")
                break

        # Fetch properties using new api
        vin = "Unknown_VIN"
        model = "EV Scooter"
        try:
            props = await api.get_scooter_properties(scooter_uuid, self.api_token)
            if props:
                vin = props.get("vin", "Unknown_VIN")
                model = props.get("model_type", "EV Scooter").strip()
        except Exception as e:
            _LOGGER.error("Failed to fetch scooter properties: %s", e)

        # Unique ID is VIN
        await self.async_set_unique_id(vin)
        self._abort_if_unique_id_configured()

        return self.async_create_entry(
            title=f"Ather {model} ({vin})",
            data={
                CONF_SCOOTER_ID: scooter_id, # Stores the short ID
                CONF_SCOOTER_UUID: scooter_uuid,
                CONF_ATHER_TOKEN: self.api_token, # store token
                "api_token": self.api_token, # Keep for backward compat
                CONF_VIN: vin,
                CONF_MODEL: model,
                CONF_FIREBASE_TOKEN: self.firebase_token or "",
                CONF_FIREBASE_API_KEY: self.api_key or "",
                CONF_NAME: name,
                CONF_MOBILE_NO: self.mobile_no,
            },
        )

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> FlowResult:
        """Handle reauthorization request."""
        self.mobile_no = entry_data.get(CONF_MOBILE_NO)
        self.api_key = entry_data.get(CONF_FIREBASE_API_KEY, "")
        self.name = entry_data.get(CONF_NAME, "Ather")

        # Store context/entry ID for update
        self.reauth_entry = self.hass.config_entries.async_get_entry(
            self.context.get("entry_id")
        )

        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Confirm reauth - request OTP."""
        errors: dict[str, str] = {}

        # If we don't have mobile_no, ask the user to verify/enter it
        if not self.mobile_no:
            if user_input is not None:
                self.mobile_no = user_input.get(CONF_MOBILE_NO)
                self.api_key = ""
            else:
                entry_data = self.reauth_entry.data if self.reauth_entry else {}
                return self.async_show_form(
                    step_id="reauth_confirm",
                    data_schema=vol.Schema(
                        {
                            vol.Required(
                                CONF_MOBILE_NO,
                                default=entry_data.get(CONF_MOBILE_NO, ""),
                            ): str,
                        }
                    ),
                    errors=errors,
                )

        if user_input is None or CONF_OTP not in user_input:
            # First phase: generate OTP
            session = async_get_clientsession(self.hass)
            api = AtherAPI(session)
            if await api.generate_otp(self.mobile_no):
                # Show OTP form
                return self.async_show_form(
                    step_id="reauth_confirm",
                    data_schema=vol.Schema({vol.Required(CONF_OTP): str}),
                    errors=errors,
                )
            else:
                errors["base"] = "otp_generation_failed"
                # If OTP generation failed, allow user to change phone number
                self.mobile_no = None
                return await self.async_step_reauth_confirm()
        else:
            # Second phase: verify OTP and update tokens
            otp = user_input[CONF_OTP]
            session = async_get_clientsession(self.hass)
            api = AtherAPI(session)
            tokens = await api.verify_otp(self.mobile_no, otp)

            if tokens:
                self.api_token = tokens.get("token")
                self.firebase_token = tokens.get("firebase_token")

                # Fetch dynamic details to update just in case
                scooter_uuid = self.reauth_entry.data.get(CONF_SCOOTER_UUID) if self.reauth_entry else None
                scooter_id = self.reauth_entry.data.get(CONF_SCOOTER_ID) if self.reauth_entry else None
                vin = self.reauth_entry.data.get(CONF_VIN) if self.reauth_entry else None
                model = self.reauth_entry.data.get(CONF_MODEL) if self.reauth_entry else None

                try:
                    scooters = await api.get_scooters_v2(self.api_token)
                    if scooters:
                        matched = None
                        if scooter_uuid:
                            for s in scooters:
                                if s.get("scooter_uuid") == scooter_uuid:
                                    matched = s
                                    break
                        if not matched:
                            matched = scooters[0]
                        scooter_uuid = matched.get("scooter_uuid")
                        scooter_id = matched.get("scooter")
                        props = await api.get_scooter_properties(scooter_uuid, self.api_token)
                        if props:
                            vin = props.get("vin", vin)
                            model = props.get("model_type", model).strip()
                except Exception as e:
                    _LOGGER.error("Reauth: failed to update vehicle information: %s", e)

                # Update config entry
                if self.reauth_entry:
                    self.hass.config_entries.async_update_entry(
                        self.reauth_entry,
                        data={
                            **self.reauth_entry.data,
                            CONF_SCOOTER_ID: scooter_id,
                            CONF_SCOOTER_UUID: scooter_uuid,
                            CONF_VIN: vin,
                            CONF_MODEL: model,
                            CONF_FIREBASE_TOKEN: self.firebase_token or "",
                            "api_token": self.api_token,
                            CONF_ATHER_TOKEN: self.api_token,
                            CONF_MOBILE_NO: self.mobile_no,
                        },
                    )
                    # Reload the integration to apply the new tokens immediately
                    await self.hass.config_entries.async_reload(
                        self.reauth_entry.entry_id
                    )
                    return self.async_abort(reason="reauth_successful")
            else:
                errors["base"] = "invalid_otp"

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({vol.Required(CONF_OTP): str}),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Create the options flow."""
        return AtherOptionsFlowHandler(config_entry)


class AtherOptionsFlowHandler(config_entries.OptionsFlow):
    """Ather Options flow handler."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_ENABLE_RAW_LOGGING,
                        default=self._config_entry.options.get(
                            CONF_ENABLE_RAW_LOGGING, DEFAULT_ENABLE_RAW_LOGGING
                        ),
                    ): bool,
                }
            ),
        )
