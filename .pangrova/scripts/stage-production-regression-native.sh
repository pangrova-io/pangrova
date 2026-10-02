#!/usr/bin/env bash
# Stage the exact classpath JNI artifact from the locally built production image.
set -euo pipefail
[[ $# -eq 1 ]] || { echo "Expected one locally built production image" >&2; exit 1; }
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
    echo "Production regression staging requires Linux x86_64" >&2; exit 1;
}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
native_stage="$(mktemp -d)"
native_container="$(docker create "$1")"
trap 'docker rm "$native_container" >/dev/null' EXIT
docker cp "$native_container:/opt/pangrova/lib" "$native_stage/lib" >&2
native_bundles=("$native_stage/lib"/pangrova-native-bundle-*-linux-x86_64.jar)
[[ ${#native_bundles[@]} -eq 1 && -f "${native_bundles[0]}" ]] || {
    echo "Production image must contain exactly one Linux x86_64 classifier" >&2; exit 1;
}
native_bundle="${native_bundles[0]}"
mkdir "$native_stage/extracted"
(
    cd "$native_stage/extracted"
    jar --extract --file "$native_bundle" META-INF/native/linux/x86_64/libpangrova_native.so
)
bash "$PROJECT_ROOT/pangrova-worker-native/bin/package-native-lib.sh" --production \
    "$native_stage/extracted/META-INF/native/linux/x86_64/libpangrova_native.so" >&2
printf '%s\n' "$native_bundle"
