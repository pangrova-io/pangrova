#!/usr/bin/env bash
# Provision the Docker endpoint for the macOS Native test job.
set -euo pipefail

arch=$(uname -m)
lima_version=2.3.0
case "$arch" in
    arm64)
        digest=b7af444bd8f91d3c307477e562f2cf3bd7595d62b9429185e4fa841563578cfc
        # GitHub-hosted ARM runners cannot run an accelerated nested VM.
        # Lima selects QEMU's software accelerator when HVF is unavailable.
        vm_type=qemu
        if ! command -v qemu-system-aarch64 >/dev/null; then
            HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 brew install qemu
        fi
        ;;
    x86_64)
        digest=d58bc7d6b8158d638720d8c8fdb17eb58527dfead6a29a5b60f48a35ee4a65a6
        vm_type=vz
        ;;
    *) printf 'Unsupported macOS runner architecture: %s\n' "$arch" >&2; exit 1 ;;
esac

tools_dir=$(mktemp -d "$RUNNER_TEMP/lima-tools.XXXXXXXX")
archive="$tools_dir/lima.tar.gz"
curl --proto '=https' --tlsv1.2 -fsSL --connect-timeout 30 --max-time 300 \
    "https://github.com/lima-vm/lima/releases/download/v$lima_version/lima-$lima_version-Darwin-$arch.tar.gz" \
    -o "$archive"
printf '%s  %s\n' "$digest" "$archive" | shasum -a 256 -c
tar -xzf "$archive" -C "$tools_dir"

export LIMA_HOME="$RUNNER_TEMP/native-lima"
lima="$tools_dir/bin/limactl"
"$lima" start --tty=false --name native-ci --vm-type "$vm_type" \
    --cpus 1 --memory 2 --disk 8 --mount-none template:docker-rootful

docker_socket="$LIMA_HOME/native-ci/sock/docker.sock"
test -S "$docker_socket"
ping=$(curl --unix-socket "$docker_socket" --connect-timeout 15 --max-time 30 -fsS http://localhost/_ping)
test "$ping" = OK
{
    printf 'DOCKER_HOST=unix://%s\n' "$docker_socket"
    printf 'TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1\n'
    printf 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock\n'
} >> "$GITHUB_ENV"
