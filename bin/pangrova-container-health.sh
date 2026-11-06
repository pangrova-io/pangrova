#!/usr/bin/env sh
# Docker image liveness probe. The image supports both node roles; custom
# management ports must be supplied through PANGROVA_HEALTH_PORT.
set -eu

command_line=$(tr '\000' ' ' < /proc/1/cmdline)
case "$command_line" in
    *org.pangrova.worker.bootstrap.PangrovaWorkerMain*) default_port=8081 ;;
    *org.pangrova.server.coordinator.bootstrap.PangrovaCoordinatorMain*|\
    *org.pangrova.standalone.PangrovaStandaloneMain*) default_port=8080 ;;
    *) echo 'Unknown Pangrova container role' >&2; exit 1 ;;
esac

port=${PANGROVA_HEALTH_PORT:-$default_port}
case "$port" in
    ''|*[!0-9]*) echo 'Invalid PANGROVA_HEALTH_PORT' >&2; exit 1 ;;
esac
if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
    echo 'Invalid PANGROVA_HEALTH_PORT' >&2
    exit 1
fi

exec curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
    "http://127.0.0.1:$port/v1/health/live" >/dev/null
