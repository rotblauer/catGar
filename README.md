# catGar

Sync Garmin watch data to InfluxDB. Pulls health and activity data from Garmin Connect and writes it as time-series points to an InfluxDB 2.x instance.

## Data synced

| Measurement        | Fields                                                                      |
|--------------------|-----------------------------------------------------------------------------|
| `daily_stats`      | steps, distance, calories, resting/avg/min/max HR, stress, body battery, intensity minutes, floors |
| `sleep`            | total/deep/light/REM/awake duration, SpO2, respiration, sleep scores        |
| `heart_rate`       | per-reading BPM throughout the day                                          |
| `activity`         | distance, duration, HR, calories, speed, elevation, cadence, power, VO2max (tagged by type & name) |
| `body_composition` | weight, BMI, body fat %, water %, muscle mass, bone mass, metabolic age, visceral fat |
| `respiration`      | avg waking/sleep respiration, highest, lowest                               |
| `spo2`             | average, lowest, latest SpO2                                                |

## Prerequisites

- Python 3.9+
- A Garmin Connect account (same credentials you use in the Garmin Connect mobile app)
- An InfluxDB 2.x server with a bucket created for Garmin data

## Setup

```bash
# Clone and install dependencies
git clone https://github.com/rotblauer/catGar.git
cd catGar
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env with your Garmin and InfluxDB details
```

### `.env` variables

| Variable           | Required | Default                 | Description                     |
|--------------------|----------|-------------------------|---------------------------------|
| `GARMIN_EMAIL`     | Yes      | —                       | Your Garmin Connect email       |
| `GARMIN_PASSWORD`  | Yes      | —                       | Your Garmin Connect password    |
| `INFLUXDB_URL`     | No       | `http://localhost:8086` | InfluxDB server URL             |
| `INFLUXDB_TOKEN`   | Yes      | —                       | InfluxDB API token              |
| `INFLUXDB_ORG`     | Yes      | —                       | InfluxDB organization           |
| `INFLUXDB_BUCKET`  | No       | `garmin`                | InfluxDB bucket name            |

## Usage

```bash
# Sync today's data
python catgar.py

# Sync a specific date
python catgar.py 2024-06-15

# Sync the last 7 days
python catgar.py --days 7

# Sync the last 30 days (initial backfill)
python catgar.py --days 30
```

### Automate with cron (macOS / Linux)

Run daily at 8 AM:

```bash
crontab -e
```

Add:

```
0 8 * * * cd /path/to/catGar && /path/to/python catgar.py >> /tmp/catgar.log 2>&1
```

### Automate with macOS launchd

Create `~/Library/LaunchAgents/com.catgar.sync.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.catgar.sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/catGar/catgar.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/catGar</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>8</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/tmp/catgar.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/catgar.log</string>
</dict>
</plist>
```

Load the agent:

```bash
launchctl load ~/Library/LaunchAgents/com.catgar.sync.plist
```

## Tests

```bash
pip install pytest
python -m pytest tests/ -v
```
