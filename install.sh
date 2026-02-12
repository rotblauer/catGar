#!/usr/bin/env bash
# install.sh — set up catGar on a Raspberry Pi (or any Linux system with systemd).
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/rotblauer/catGar/main/install.sh | bash
#   # — or —
#   git clone https://github.com/rotblauer/catGar.git && cd catGar && bash install.sh
#
# What it does:
#   1. Installs system packages (python3, pip, venv)
#   2. Clones or copies the repo to /opt/catgar
#   3. Creates a Python virtual environment and installs dependencies
#   4. Prompts for .env configuration (if not already present)
#   5. Installs systemd service + timer for 4-hour sync
#   6. Runs an initial backfill of all available data

set -euo pipefail

INSTALL_DIR="/opt/catgar"
REPO_URL="https://github.com/rotblauer/catGar.git"

info()  { printf '\033[1;32m[catGar]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[catGar]\033[0m %s\n' "$*"; }
error() { printf '\033[1;31m[catGar]\033[0m %s\n' "$*" >&2; }

# --- Ensure root ---
if [ "$(id -u)" -ne 0 ]; then
    error "Please run as root:  sudo bash install.sh"
    exit 1
fi

# --- System dependencies ---
info "Installing system packages…"
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git > /dev/null

# --- Deploy application ---
if [ -f "catgar.py" ]; then
    # Running from within the cloned repo
    info "Copying project to ${INSTALL_DIR}…"
    mkdir -p "${INSTALL_DIR}"
    cp catgar.py requirements.txt .env.example catgar.service catgar.timer "${INSTALL_DIR}/"
else
    # Fresh download
    if [ -d "${INSTALL_DIR}/.git" ]; then
        info "Updating existing install…"
        git -C "${INSTALL_DIR}" pull --ff-only
    else
        info "Cloning repository to ${INSTALL_DIR}…"
        git clone "${REPO_URL}" "${INSTALL_DIR}"
    fi
fi

# --- Python venv + deps ---
info "Setting up Python virtual environment…"
python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
"${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
info "Python dependencies installed."

# --- .env configuration ---
if [ ! -f "${INSTALL_DIR}/.env" ]; then
    info "No .env file found — let's configure credentials."
    cp "${INSTALL_DIR}/.env.example" "${INSTALL_DIR}/.env"

    read -rp "  Garmin Connect email: " GARMIN_EMAIL
    read -rsp "  Garmin Connect password: " GARMIN_PASSWORD
    echo
    read -rp "  InfluxDB URL [http://localhost:8086]: " INFLUXDB_URL
    INFLUXDB_URL="${INFLUXDB_URL:-http://localhost:8086}"
    read -rp "  InfluxDB API token: " INFLUXDB_TOKEN
    read -rp "  InfluxDB organization: " INFLUXDB_ORG
    read -rp "  InfluxDB bucket [garmin]: " INFLUXDB_BUCKET
    INFLUXDB_BUCKET="${INFLUXDB_BUCKET:-garmin}"

    cat > "${INSTALL_DIR}/.env" <<EOF
GARMIN_EMAIL=${GARMIN_EMAIL}
GARMIN_PASSWORD=${GARMIN_PASSWORD}
INFLUXDB_URL=${INFLUXDB_URL}
INFLUXDB_TOKEN=${INFLUXDB_TOKEN}
INFLUXDB_ORG=${INFLUXDB_ORG}
INFLUXDB_BUCKET=${INFLUXDB_BUCKET}
EOF
    chmod 600 "${INSTALL_DIR}/.env"
    info ".env written to ${INSTALL_DIR}/.env"
else
    info "Existing .env found — skipping credential setup."
fi

# --- systemd units ---
info "Installing systemd service and timer…"
cp "${INSTALL_DIR}/catgar.service" /etc/systemd/system/catgar.service
cp "${INSTALL_DIR}/catgar.timer"   /etc/systemd/system/catgar.timer
systemctl daemon-reload
systemctl enable catgar.timer
systemctl start catgar.timer
info "catgar.timer enabled and started (runs every 4 hours)."

# --- Initial backfill ---
info "Running initial backfill (all available data). This may take a while…"
if "${INSTALL_DIR}/venv/bin/python" "${INSTALL_DIR}/catgar.py" --backfill; then
    info "Backfill complete!"
else
    warn "Backfill finished with errors — check the logs above."
    warn "The timer is still active; future syncs will resume automatically."
fi

info ""
info "=== catGar is installed and running ==="
info "  Install dir : ${INSTALL_DIR}"
info "  Timer       : systemctl status catgar.timer"
info "  Logs        : journalctl -u catgar.service"
info "  Manual run  : sudo systemctl start catgar.service"
info ""
