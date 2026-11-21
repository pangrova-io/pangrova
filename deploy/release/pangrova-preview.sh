#!/usr/bin/env bash
# Start the verified Linux x86_64 release archive in a disposable Docker network.
set -euo pipefail
umask 077

label=io.pangrova.preview

cleanup() {
    local trial="$1" name owned status=0
    for name in "$trial-runtime" "$trial-db"; do
        owned=$(docker container ls -aq --filter "name=^/$name$" --filter "label=$label=$trial") || status=1
        if [[ -n "$owned" ]]; then
            docker rm -fv "$name" >/dev/null || status=1
        fi
    done
    owned=$(docker network ls -q --filter "name=^$trial$" --filter "label=$label=$trial") || status=1
    if [[ -n "$owned" ]]; then
        docker network rm "$trial" >/dev/null || status=1
    fi
    owned=$(docker image ls -q --filter "reference=$trial:preview" --filter "label=$label=$trial") || status=1
    if [[ -n "$owned" ]]; then
        docker image rm "$trial:preview" >/dev/null || status=1
    fi
    return "$status"
}

if [[ ${1:-} == stop && $# == 2 && "$2" =~ ^pangrova-preview-[0-9a-f]{16}$ ]]; then
    cleanup "$2"
    exit 0
fi
if [[ ! "${2:-}" =~ ^[0-9][A-Za-z0-9._+-]*$ || ! "${3:-}" =~ ^[0-9a-f]{64}$ ]]; then
    printf 'Usage: %s start VERSION SHA256 | stop TRIAL_ID | verify VERSION SHA256 ARCHIVE TOPOLOGY\n' "$0" >&2
    exit 2
fi
case "$1:$#" in
    start:3) topology=standalone ;;
    verify:5)
        case "$5" in standalone|split) topology="$5" ;; *) exit 2 ;; esac
        ;;
    *) exit 2 ;;
esac

[[ "$(uname -s):$(uname -m)" == Linux:x86_64 ]] || {
    echo 'Preview requires a Linux x86_64 host.' >&2
    exit 1
}
engine=$(docker info --format '{{.OSType}}/{{.Architecture}}')
case "$engine" in
    linux/x86_64|linux/amd64) ;;
    *)
        echo 'Preview requires a local Linux x86_64 Docker engine.' >&2
        exit 1
        ;;
esac

version="$2"
digest="$3"
name="pangrova-$version-linux-x86_64"
trial=pangrova-preview-$(od -An -N8 -tx1 /dev/urandom | tr -d '[:space:]')
work=$(mktemp -d "${TMPDIR:-/tmp}/pangrova-preview-XXXXXXXX")
on_exit() {
    local status=$?
    trap - EXIT
    cleanup "$trial" || status=1
    rm -rf -- "$work"
    exit "$status"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Preview ID: $trial"
started=$SECONDS
if [[ "$1" == verify ]]; then
    cp -- "$4" "$work/$name.tar.gz"
else
    url="https://github.com/pangrova-io/pangrova/releases/download/v$version/$name.tar.gz"
    curl --fail --silent --show-error --location --connect-timeout 15 --max-time 600 \
        --output "$work/$name.tar.gz" "$url"
fi
printf '%s  %s\n' "$digest" "$work/$name.tar.gz" | sha256sum --check
tar -xzf "$work/$name.tar.gz" -C "$work"
cat "$work/$name/manifest.json"

docker build --platform linux/amd64 --label "$label=$trial" --tag "$trial:preview" \
    "$work/$name/preview"
docker network create --internal --label "$label=$trial" "$trial" >/dev/null
POSTGRES_PASSWORD=$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')
PANGROVA_PREVIEW_DB_PASSWORD=$POSTGRES_PASSWORD
export POSTGRES_PASSWORD PANGROVA_PREVIEW_DB_PASSWORD
docker run --detach --platform linux/amd64 --name "$trial-db" --label "$label=$trial" \
    --network "$trial" --network-alias metadata --env POSTGRES_PASSWORD \
    --env POSTGRES_USER=pangrova --env POSTGRES_DB=pangrova_meta postgres:16-alpine >/dev/null

deadline=$((SECONDS + 60))
until docker exec "$trial-db" pg_isready -q -h 127.0.0.1 -U pangrova -d pangrova_meta; do
    if ((SECONDS >= deadline)); then
        docker logs "$trial-db" >&2
        echo 'Preview PostgreSQL did not become ready within 60 seconds.' >&2
        exit 1
    fi
    sleep 1
done

docker run --platform linux/amd64 --init --name "$trial-runtime" --label "$label=$trial" \
    --network "$trial" --env PANGROVA_PREVIEW_DB_PASSWORD \
    --env "PANGROVA_PREVIEW_TOPOLOGY=$topology" \
    --mount "type=bind,src=$work/$name,dst=/opt/pangrova,readonly" \
    "$trial:preview" python3 /opt/pangrova/preview/run.py
cleanup "$trial"
echo "Preview finished in $((SECONDS - started))s. Docker trial resources removed."
