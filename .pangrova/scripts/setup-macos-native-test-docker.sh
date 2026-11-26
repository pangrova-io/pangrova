#!/usr/bin/env bash
# Provision the Docker endpoint for the macOS Native test job.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
arch=$(uname -m)
lima_version=2.3.0
docker_version=29.10.1
case "$arch" in
    arm64)
        lima_digest=b7af444bd8f91d3c307477e562f2cf3bd7595d62b9429185e4fa841563578cfc
        docker_arch=aarch64
        docker_digest=a38c8a9ef6d17bce07499bfd3a3688542274769dc3b4f75c06ea2705fe65053e
        # GitHub-hosted ARM runners cannot run an accelerated nested VM.
        # Lima selects QEMU's software accelerator when HVF is unavailable.
        vm_type=qemu
        if ! command -v qemu-system-aarch64 >/dev/null; then
            HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 brew install qemu
        fi
        ;;
    x86_64)
        lima_digest=d58bc7d6b8158d638720d8c8fdb17eb58527dfead6a29a5b60f48a35ee4a65a6
        docker_arch=x86_64
        docker_digest=061642059fa7f2ed9b2f04a327a951cff4249585cf04b2661c2a6539197c9962
        vm_type=vz
        ;;
    *) printf 'Unsupported macOS runner architecture: %s\n' "$arch" >&2; exit 1 ;;
esac

tools_dir=$(mktemp -d "$RUNNER_TEMP/lima-tools.XXXXXXXX")
archive="$tools_dir/lima.tar.gz"
curl --proto '=https' --tlsv1.2 -fsSL --connect-timeout 30 --max-time 300 \
    "https://github.com/lima-vm/lima/releases/download/v$lima_version/lima-$lima_version-Darwin-$arch.tar.gz" \
    -o "$archive"
printf '%s  %s\n' "$lima_digest" "$archive" | shasum -a 256 -c
tar -xzf "$archive" -C "$tools_dir"

# Fetch and verify the Engine on the host before booting the guest. The
# software-emulated guest can spend its boot budget starting Linux.
docker_archive="$tools_dir/docker.tgz"
curl --http1.1 --proto '=https' --tlsv1.2 -fsSL --connect-timeout 30 --max-time 300 \
    "https://download.docker.com/linux/static/stable/$docker_arch/docker-$docker_version.tgz" \
    -o "$docker_archive"
printf '%s  %s\n' "$docker_digest" "$docker_archive" | shasum -a 256 -c

export LIMA_HOME="$RUNNER_TEMP/native-lima"
lima="$tools_dir/bin/limactl"
instance_dir="$LIMA_HOME/native-ci"

# Lima's startup result identifies the failing readiness stage. Keep the
# host and guest evidence even when the instance never reaches running.
report_startup_failure() {
    local log
    for log in "$instance_dir"/ha.stderr.log "$instance_dir"/serial*.log; do
        if [[ -s "$log" ]]; then
            printf 'Lima startup log %s\n' "${log##*/}" >&2
            tail -c 16384 "$log" >&2
            printf '\n' >&2
        fi
    done
    if [[ -r "$instance_dir/ssh.config" ]]; then
        python3 - "$instance_dir/ssh.config" <<'PYTHON'
import subprocess
import sys

try:
    result = subprocess.run(
        ["ssh", "-F", sys.argv[1], "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
         "lima-native-ci",
         "sudo -n tail -c 16384 /var/log/cloud-init-output.log; "
         "sudo -n journalctl -u pangrova-docker --no-pager -n 100"],
        timeout=10,
    )
    if result.returncode:
        print("Lima guest diagnostics are incomplete", file=sys.stderr)
except (OSError, subprocess.TimeoutExpired) as error:
    print(f"Could not read Lima guest diagnostics: {error}", file=sys.stderr)
PYTHON
    fi
}

trap 'status=$?; set +e; report_startup_failure; exit "$status"' ERR
"$lima" start --tty=false --name native-ci --vm-type "$vm_type" \
    --cpus 1 --memory 2 --disk 8 --mount-none "$script_dir/setup-macos-native-test-docker.yaml"
"$lima" shell --workdir / native-ci sudo -n tar -xzf - -C /opt < "$docker_archive"
"$lima" shell --workdir / native-ci sh -eu <<'GUEST'
sudo -n systemd-run --unit=pangrova-docker --property=Type=notify --property=Delegate=yes \
    --setenv=PATH=/opt/docker:/usr/sbin:/usr/bin:/sbin:/bin \
    /opt/docker/dockerd --group="$(id -gn)"
GUEST

docker_socket="$instance_dir/sock/docker.sock"
test -S "$docker_socket"
ping=$(curl --unix-socket "$docker_socket" --connect-timeout 15 --max-time 30 -fsS http://localhost/_ping)
test "$ping" = OK
trap - ERR
{
    printf 'DOCKER_HOST=unix://%s\n' "$docker_socket"
    printf 'TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1\n'
    printf 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock\n'
} >> "$GITHUB_ENV"
