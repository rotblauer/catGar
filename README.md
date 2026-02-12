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
# Sync since last sync (or today if first run)
python catgar.py

# Sync a specific date
python catgar.py 2024-06-15

# Sync the last 7 days
python catgar.py --days 7

# Initial backfill — all available data (up to 5 years)
python catgar.py --backfill
```

### Raspberry Pi quick install

If you have an existing InfluxDB instance running on a Raspberry Pi, a single
command handles everything — system packages, Python venv, credentials, systemd
timer (every 4 hours), and an initial full backfill:

```bash
git clone https://github.com/rotblauer/catGar.git
cd catGar
sudo bash install.sh
```

The installer will:

1. Install `python3`, `pip`, `venv`, and `git` via `apt`.
2. Copy the project to `/opt/catgar` and create a virtual environment.
3. Prompt you for Garmin + InfluxDB credentials (writes `/opt/catgar/.env`).
4. Install a **systemd timer** that runs the sync every **4 hours**.
5. Kick off an initial **backfill** of all available historical data.

After install you can check status with:

```bash
# Timer status
systemctl status catgar.timer

# Last run output
journalctl -u catgar.service -e

# Trigger a manual sync
sudo systemctl start catgar.service
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
