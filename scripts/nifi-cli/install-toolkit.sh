#!/usr/bin/env bash
# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Install NiFi Toolkit 2.8.0
# =============================================================================
# Usage: ./scripts/nifi-cli/install-toolkit.sh
# Downloads and installs NiFi Toolkit CLI to ./tools/nifi-toolkit/.
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Colors and helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NIFI_VERSION="2.8.0"
TOOLKIT_NAME="nifi-toolkit-${NIFI_VERSION}"
TOOLKIT_ARCHIVE="${TOOLKIT_NAME}-bin.zip"
TOOLKIT_DIR="${PROJECT_ROOT}/tools/nifi-toolkit"
TOOLKIT_CLI="${TOOLKIT_DIR}/bin/cli.sh"

# Apache mirror URLs (try multiple)
MIRROR_URLS=(
    "https://downloads.apache.org/nifi/${NIFI_VERSION}/${TOOLKIT_ARCHIVE}"
    "https://archive.apache.org/dist/nifi/${NIFI_VERSION}/${TOOLKIT_ARCHIVE}"
    "https://dlcdn.apache.org/nifi/${NIFI_VERSION}/${TOOLKIT_ARCHIVE}"
)

echo -e "${BOLD}========================================================${NC}"
echo -e "${BOLD}  NiFi Toolkit ${NIFI_VERSION} - Installation           ${NC}"
echo -e "${BOLD}========================================================${NC}"
echo ""

# ---------------------------------------------------------------------------
# Check if already installed
# ---------------------------------------------------------------------------
if [[ -f "${TOOLKIT_CLI}" ]]; then
    success "NiFi Toolkit already installed at ${TOOLKIT_DIR}"
    info "Version check:"
    "${TOOLKIT_CLI}" --help 2>&1 | head -3 || true
    echo ""
    read -rp "Reinstall? (yes/no): " REINSTALL
    if [[ "${REINSTALL}" != "yes" ]]; then
        info "Keeping existing installation."
        exit 0
    fi
    rm -rf "${TOOLKIT_DIR}"
fi

# ---------------------------------------------------------------------------
# Check prerequisites
# ---------------------------------------------------------------------------
if ! command -v java &>/dev/null; then
    warn "Java not found. NiFi Toolkit requires Java 21+."
    warn "Install with: apt install openjdk-21-jre-headless (Debian/Ubuntu)"
    warn "              brew install openjdk@21 (macOS)"
fi

if ! command -v unzip &>/dev/null; then
    error "unzip is required but not installed. Install with: apt install unzip"
fi

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
TEMP_DIR=$(mktemp -d)
DOWNLOAD_PATH="${TEMP_DIR}/${TOOLKIT_ARCHIVE}"

info "Downloading NiFi Toolkit ${NIFI_VERSION}..."

DOWNLOADED=false
for url in "${MIRROR_URLS[@]}"; do
    info "  Trying: ${url}"
    if curl -fSL "${url}" -o "${DOWNLOAD_PATH}" --progress-bar 2>/dev/null; then
        DOWNLOADED=true
        success "Downloaded from $(echo "${url}" | cut -d/ -f3)"
        break
    fi
done

if ! ${DOWNLOADED}; then
    rm -rf "${TEMP_DIR}"
    error "Failed to download NiFi Toolkit from any mirror."
fi

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
info "Extracting NiFi Toolkit..."

mkdir -p "${PROJECT_ROOT}/tools"
unzip -q "${DOWNLOAD_PATH}" -d "${TEMP_DIR}"

# Find extracted directory (name varies by version)
EXTRACTED_DIR=$(find "${TEMP_DIR}" -maxdepth 1 -type d -name "nifi-toolkit-*" | head -1)

if [[ -z "${EXTRACTED_DIR}" ]]; then
    rm -rf "${TEMP_DIR}"
    error "Failed to find extracted toolkit directory."
fi

# Move to final location
mv "${EXTRACTED_DIR}" "${TOOLKIT_DIR}"
success "Extracted to ${TOOLKIT_DIR}"

# ---------------------------------------------------------------------------
# Make executable
# ---------------------------------------------------------------------------
chmod +x "${TOOLKIT_DIR}/bin/"*.sh 2>/dev/null || true
success "Set executable permissions on CLI scripts."

# ---------------------------------------------------------------------------
# Create convenience symlink
# ---------------------------------------------------------------------------
SYMLINK="${PROJECT_ROOT}/tools/nifi-cli"

if [[ -L "${SYMLINK}" ]] || [[ -f "${SYMLINK}" ]]; then
    rm -f "${SYMLINK}"
fi

ln -sf "${TOOLKIT_DIR}/bin/cli.sh" "${SYMLINK}" 2>/dev/null || true

if [[ -L "${SYMLINK}" ]]; then
    success "Created symlink: tools/nifi-cli -> nifi-toolkit/bin/cli.sh"
fi

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
rm -rf "${TEMP_DIR}"

# ---------------------------------------------------------------------------
# Verify installation
# ---------------------------------------------------------------------------
info "Verifying installation..."
echo ""

if [[ -f "${TOOLKIT_CLI}" ]]; then
    "${TOOLKIT_CLI}" --help 2>&1 | head -5 || true
    echo ""
    success "NiFi Toolkit ${NIFI_VERSION} installed successfully."
else
    error "Installation verification failed. CLI not found at ${TOOLKIT_CLI}"
fi

echo ""
echo -e "${BOLD}Usage examples:${NC}"
echo -e "  ${CYAN}./tools/nifi-cli registry list-buckets --baseUrl http://localhost:18080${NC}"
echo -e "  ${CYAN}./tools/nifi-cli nifi get-root-id --baseUrl https://localhost:8443${NC}"
echo ""
