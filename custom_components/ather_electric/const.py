"""Constants for the Ather Electric integration."""

DOMAIN = "ather_electric"

CONF_SCOOTER_ID = "scooter_id"
CONF_FIREBASE_TOKEN = "firebase_token"
CONF_FIREBASE_API_KEY = "firebase_api_key"
CONF_ATHER_TOKEN = "ather_token"
CONF_SCOOTER_UUID = "scooter_uuid"
CONF_VIN = "vin"
CONF_MODEL = "model"
CONF_BASE_URL = "base_url"

# WebSocket URL
WS_URL = "wss://ather-production.firebaseio.com/.ws?v=5"
WS_ENDPOINT = "wss://cerberus.ather.io/api/v1/ws/devices/shadows/onchange"

# Platforms
PLATFORMS = ["sensor", "device_tracker", "binary_sensor", "button", "switch"]

CONF_MOBILE_NO = "mobile_no"
CONF_OTP = "otp"

# API URLs
BASE_URL = "https://cerberus.ather.io"
GENERATE_OTP_URL = "https://cerberus.ather.io/auth/v2/generate-login-otp"
VERIFY_OTP_URL = "https://cerberus.ather.io/auth/v2/verify-login-otp"
TOKEN_VERIFY_URL = (
    "https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyCustomToken"
)
TOKEN_REFRESH_URL = "https://securetoken.googleapis.com/v1/token"
ME_URL = "https://cerberus.ather.io/api/v1/me"
RIDES_URL = "https://cerberus.ather.io/api/v1/rides"

# Headers
HEADERS_BASE = {
    "Source": "ATHER_APP/13.1.0",
    "X-Platform": "Android",
    "X-Platform-Version": "11",
    "X-Device-Info": "Google Pixel 4",
    "User-Agent": "Android/11 (Google Pixel 4)",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip",
}

COMMON_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Source": "ATHER_APP/13.2.0",
    "User-Agent": "okhttp/4.12.0",
    "X-Android-Package": "com.athermobileapp",
    "X-Android-Cert": "385607f06926ced3c0630914bc6b78ac2ba99211",
}

CONF_ENABLE_RAW_LOGGING = "enable_raw_logging"
DEFAULT_ENABLE_RAW_LOGGING = False

CONF_RIDE_RETENTION_MONTHS = "ride_retention_months"
DEFAULT_RIDE_RETENTION_MONTHS = 13
