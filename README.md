# Ather Electric - Home Assistant Integration

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg?style=for-the-badge)](https://github.com/hacs/default)
[![GitHub Release](https://img.shields.io/github/v/release/Tilak-Sidduram/ather_electric?style=for-the-badge&color=blue)](https://github.com/Tilak-Sidduram/ather_electric)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg?style=for-the-badge)](LICENSE)
[![Home Assistant](https://img.shields.io/badge/Home_Assistant-2024.1%2B-blueviolet.svg?style=for-the-badge)](https://www.home-assistant.io/)

A comprehensive custom integration for **Ather Electric** scooters (**450X**, **450 Plus**, **450 Apex**, and **Rizta**) in [Home Assistant](https://www.home-assistant.io/).

Connects directly via Ather's native **Cerberus Cloud & WebSocket API** to provide real-time telemetry, vehicle health analytics (TrueHealth™), tire pressure monitoring (TPMS), historical ride analytics, GPS device tracking, and remote vehicle controls.

---

## Features

### ⚡ Real-Time Telemetry
* **Battery State of Charge (SOC %)**: Real-time battery percentage with dynamic charging state detection.
* **Estimated Range (Live & Projected)**: Live estimated remaining range, plus dynamically calculated projected range for all riding modes (**Eco**, **Ride**, **Sport**, **Warp**, and **SmartEco**).
* **Speed & Odometer**: Current speed (km/h) and total lifetime odometer reading.
* **Riding Mode & Vehicle State**: Active riding mode and vehicle operating state (`standby`, `riding`, `charging`, `park`).

### 🩺 Ather TrueHealth™ Diagnostics & Vehicle Health Suite
* **TrueHealth Score (0–100%)**: Composite health score evaluating powertrain, battery, braking, and transmission.
* **TrueHealth Rating Badge**: Real-time evaluation badge (`Optimal`, `Healthy`, `Fair`).
* **Battery State of Health (SoH %)**: Battery health percentage tracking charge cycle count, thermal status (°C), cell balance drift (<12mV), and degradation percentage.
* **Eight70™ Battery Warranty Tracker**: Tracks Ather's official 8-Year / 80,000 km battery health guarantee (≥ 70% SoH), displaying coverage status and remaining warranty distance.
* **Subsystem Wear Scores**:
  * **Electric Motor & MCU Health (0–100%)**
  * **Brake Pads & Disc Wear (0–100%)**: Computed based on odometer wear and credited by regenerative braking / coasting percentage.
  * **Gates Carbon Drive Belt Health (0–100%)**: Tracks the ~25,000 km belt tension and replacement cycle.
  * **Tyres & TPMS Margins (0–100%)**
* **Dynamic Certified Resale Valuation**: Real-time market valuation in INR (₹) including TrueHealth bonus valuation.

### 🛞 Tire Pressure Monitoring System (TPMS)
* **Live Tire Pressures in PSI**: Real-time front and rear tire pressures with automatic kPa to PSI unit conversion.
* **TPMS Battery Voltage**: Sensor health and battery level for both wheel sensors.
* **Safety Hazard & Alert Flags**: Binary alert sensors for front/rear low pressure, high pressure, and critical safety hazards.

### 🔌 Charging & Power
* **Charger Connected & Charging Status**: Immediate detection when plugged into home or Ather Grid fast chargers.
* **Charger Type & Heartbeat**: Identifies standard home pods, portable chargers, or DC fast charging grids.

### 🎮 Remote Vehicle Controls
* **Remote Charging Switch**: Start or stop charging sessions directly from Home Assistant.
* **Ping My Scooter Button**: Flashes indicators and headlamps to easily locate your scooter in parking lots.
* **Remote Shutdown Button**: Remotely powers down the scooter's onboard computers to conserve battery during vacation/standby.
* **Shutdown Protection Switch (Safe Mode)**: Software safety interlock to prevent accidental remote shutdowns.

### 📍 GPS Tracking & Device Tracker
* **Live GPS Coordinates**: Updates latitude, longitude, and accuracy via the native `device_tracker` platform.
* **Custom Vehicle Branding**: Displays Ather scooter icon and vehicle artwork in Home Assistant maps and dashboards.

### 📊 Historical Rides & Ride Manager
* **Automated Post-Ride Sync**: Automatically syncs distance, duration, top speed, average speed, and efficiency (Wh/km & km/kWh) after each ride.
* **Daily Syncing**: Keeps ride histories and utility meters in sync with the cloud database.

---

## Supported Vehicles

* **Ather 450X** (Gen 1, Gen 2, Gen 3, Gen 3.5, Gen 4)
* **Ather 450 Plus**
* **Ather 450 Apex**
* **Ather Rizta** (S / Z / 2.9 kWh / 3.7 kWh)

---

## Installation

### Method 1: HACS (Recommended)

1. Ensure [HACS](https://hacs.xyz/) is installed in your Home Assistant instance.
2. Open **HACS** > **Integrations** > Click the three dots (top right) > **Custom repositories**.
3. Paste your repository URL (e.g. `https://github.com/tilaksidduram/ather_electric`).
4. Set category to **Integration** and click **Add**.
5. Find **Ather Electric**, click **Download**, and restart Home Assistant.

### Method 2: Manual Installation

1. Download the latest release from the [Releases](https://github.com/tilaksidduram/ather_electric) page.
2. Extract the archive and copy the `ather_electric` folder into your Home Assistant directory under:
   ```text
   config/custom_components/ather_electric/
   ```
3. Restart Home Assistant.

---

## Configuration

1. In Home Assistant, navigate to **Settings** > **Devices & Services** > **Add Integration**.
2. Search for **Ather Electric** and select it.
3. Enter your **Ather registered 10-digit Mobile Number** (e.g., `9876543210`).
4. Enter the **OTP** received via SMS or WhatsApp.
5. If multiple scooters are linked to your account, select the vehicle you wish to add.
6. Click **Submit** to finalize the setup.

---

## Entities Summary

### Sensors (`sensor.*`)
| Entity ID | Name | Unit / Type | Description |
| :--- | :--- | :--- | :--- |
| `sensor.ather_<id>_battery` | Battery | `%` | Live Battery State of Charge |
| `sensor.ather_<id>_range` | Estimated Range | `km` | Remaining estimated range |
| `sensor.ather_<id>_speed` | Speed | `km/h` | Current vehicle speed |
| `sensor.ather_<id>_odo` | Odometer | `km` | Lifetime distance traveled |
| `sensor.ather_<id>_riding_mode` | Riding Mode | String | Active mode (`Eco`, `Ride`, `Sport`, `Warp`, `SmartEco`) |
| `sensor.ather_<id>_vehicle_state` | Vehicle State | String | Operating state (`standby`, `riding`, `charging`) |
| `sensor.ather_<id>_front_tyre_pressure` | Front Tyre Pressure | `psi` | Front wheel tire pressure |
| `sensor.ather_<id>_rear_tyre_pressure` | Rear Tyre Pressure | `psi` | Rear wheel tire pressure |
| `sensor.ather_<id>_truehealth_score` | TrueHealth Score | `%` | Overall composite health score |
| `sensor.ather_<id>_truehealth_rating` | TrueHealth Rating | String | Rating badge (`Optimal`, `Healthy`, `Fair`) |
| `sensor.ather_<id>_battery_soh` | Battery State of Health | `%` | Battery health with cycle and thermal telemetry |
| `sensor.ather_<id>_eight70_warranty` | Eight70 Battery Warranty | String | 8-Year / 80k km battery warranty tracker |
| `sensor.ather_<id>_motor_health` | Motor Health | `%` | Motor & MCU powertrain score |
| `sensor.ather_<id>_brake_pad_health` | Brake Pad Health | `%` | Brake pads and disc wear score |
| `sensor.ather_<id>_drive_belt_health` | Drive Belt Health | `%` | Gates Carbon Drive Belt wear score |
| `sensor.ather_<id>_tyres_health` | Tyres Health | `%` | Tyres & TPMS operational margin |
| `sensor.ather_<id>_resale_valuation` | Estimated Resale Valuation | `INR` | Certified dynamic resale market value |
| `sensor.ather_<id>_eco_projected_range` | Projected Range (Eco) | `km` | Dynamically calculated Eco range |
| `sensor.ather_<id>_ride_projected_range` | Projected Range (Ride) | `km` | Dynamically calculated Ride range |
| `sensor.ather_<id>_sport_projected_range` | Projected Range (Sport) | `km` | Dynamically calculated Sport range |
| `sensor.ather_<id>_warp_projected_range` | Projected Range (Warp) | `km` | Dynamically calculated Warp range |

### Binary Sensors (`binary_sensor.*`)
| Entity ID | Name | Description |
| :--- | :--- | :--- |
| `binary_sensor.ather_<id>_charger_connected` | Charger Connected | `On` when charging cable is plugged in |
| `binary_sensor.ather_<id>_charging_status` | Charging Status | `On` when actively drawing power |
| `binary_sensor.ather_<id>_key_switch` | Key Switch | `On` when ignition/key is on |
| `binary_sensor.ather_<id>_incognito_mode` | Incognito Mode | `On` when location tracking is paused |
| `binary_sensor.ather_<id>_smart_eco_mode` | Smart Eco Mode | Active status of Smart Eco system |
| `binary_sensor.ather_<id>_front_tyre_low_pressure` | Front Tyre Low Pressure | `On` if front tire pressure is low |
| `binary_sensor.ather_<id>_rear_tyre_low_pressure` | Rear Tyre Low Pressure | `On` if rear tire pressure is low |

### Controls & Tracking
| Entity ID | Platform | Description |
| :--- | :--- | :--- |
| `switch.ather_<id>_remote_charging` | Switch | Start / stop remote charging |
| `switch.ather_<id>_shutdown_protection` | Switch | Safety lock protecting remote shutdown |
| `button.ather_<id>_ping_my_scooter` | Button | Flashes indicators/lights to find vehicle |
| `button.ather_<id>_remote_shutdown` | Button | Remotely shuts down vehicle computers |
| `device_tracker.ather_<id>` | Device Tracker | Real-time GPS location tracking |

---

## Example Automations

### 1. Low Tire Pressure Notification
```yaml
alias: "Ather: Low Tire Pressure Alert"
trigger:
  - platform: numeric_state
    entity_id: sensor.ather_scooter_front_tyre_pressure
    below: 26
  - platform: numeric_state
    entity_id: sensor.ather_scooter_rear_tyre_pressure
    below: 28
action:
  - service: notify.notify
    data:
      title: "⚠️ Ather Scooter Tire Pressure Alert"
      message: >
        Tire pressure warning! Front: {{ states('sensor.ather_scooter_front_tyre_pressure') }} PSI,
        Rear: {{ states('sensor.ather_scooter_rear_tyre_pressure') }} PSI.
```

### 2. Charging Completed Notification
```yaml
alias: "Ather: Battery Charging Finished"
trigger:
  - platform: state
    entity_id: binary_sensor.ather_scooter_charging_status
    from: "on"
    to: "off"
condition:
  - condition: numeric_state
    entity_id: sensor.ather_scooter_battery
    above: 98
action:
  - service: notify.notify
    data:
      title: "⚡ Ather Charged"
      message: "Your scooter is fully charged ({{ states('sensor.ather_scooter_battery') }}%) and ready to ride!"
```

---

## Disclaimer

This is a community-developed, open-source custom integration. It is not an official product of **Ather Energy Pvt. Ltd.** Ather Energy and its respective model names are trademarks of Ather Energy Pvt. Ltd.

---

## License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.
