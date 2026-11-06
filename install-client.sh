#!/usr/bin/env bash
# Installs the ipcv client as a systemd service.
# the client binary, should be named as client-linux-x86_64,
# client-linux-aarch64), and ipcv-client.service in the same folder as this script.
# running it again updates an existing install.
# the script should be run with sudo
set -euo pipefail

SERVICE=ipcv-client.service
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
    echo "Error: $*" >&2
    exit 1
}

if [[ $EUID -ne 0 ]]; then
    exec sudo bash "${BASH_SOURCE[0]}" "$@"
fi

[[ -d /run/systemd/system ]] || die "this system is not running systemd"

UNIT="$DIR/$SERVICE"
[[ -f $UNIT ]] || die "$SERVICE not found in $DIR"

# searching for the correct binary for the current architercure
ARCH="$(uname -m)"
BIN="$DIR/client-linux-$ARCH"
if [[ ! -f $BIN ]]; then
    found=("$DIR"/client-linux-*)
    [[ -e ${found[0]} ]] && die "this machine is $ARCH, but $DIR only has: ${found[*]##*/}"
    die "client-linux-$ARCH not found in $DIR"
fi

# Must match ExecStart in ipcv-client.service
BIN_DEST=/usr/local/bin/ipcv-client


getent group video >/dev/null ||
    echo "Warning: there is no 'video' group, the service may not be able to open the camera" >&2
command -v ffmpeg >/dev/null ||
    echo "Warning: ffmpeg is not installed, the client needs it to stream the camera" >&2

# install
install -m 755 "$BIN" "$BIN_DEST"
install -m 644 "$UNIT" "/etc/systemd/system/$SERVICE"

systemctl daemon-reload
systemctl enable "$SERVICE"
# Starts it the first time
systemctl restart "$SERVICE"


echo "ipcv client installed."
echo "Logs: journalctl -u $SERVICE -f"
