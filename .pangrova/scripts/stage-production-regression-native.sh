#!/usr/bin/env bash
# Stage the exact runtime JAR inventory and canonical JNI from one immutable production image.
set -euo pipefail
export LC_ALL=C
[[ $# -eq 3 ]] || {
    echo "Expected image ID, empty stage directory, and source commit" >&2; exit 1;
}
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
    echo "Production regression staging requires Linux x86_64" >&2; exit 1;
}
image_id="$1"
native_stage="$2"
source_commit="$3"
[[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ && "$source_commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "Image digest or source commit is not canonical" >&2; exit 1;
}
[[ -d "$native_stage" && -z "$(find "$native_stage" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
    echo "Stage directory must exist and be empty" >&2; exit 1;
}
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "$script_dir/../.." && pwd)"
[[ "$(git -C "$project_root" rev-parse HEAD)" == "$source_commit" ]] || {
    echo "Checked-out source differs from production-job commit" >&2; exit 1;
}
[[ "$(docker image inspect --format '{{.Id}}' "$image_id")" == "$image_id" ]] || {
    echo "Production image ID changed before staging" >&2; exit 1;
}
[[ "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image_id")" == "$source_commit" ]] || {
    echo "Production image source-revision label differs from checkout" >&2; exit 1;
}
native_container="$(docker create "$image_id")"
trap 'docker rm "$native_container" >/dev/null' EXIT
docker cp "$native_container:/opt/pangrova/lib" "$native_stage/lib" >&2
shopt -s nullglob
jars=("$native_stage/lib"/*.jar)
[[ ${#jars[@]} -gt 0 ]] || { echo "Production image has no runtime JARs" >&2; exit 1; }
native_bundles=("$native_stage/lib"/pangrova-native-bundle-*-linux-x86_64.jar)
[[ ${#native_bundles[@]} -eq 1 ]] || {
    echo "Production image must contain exactly one Linux x86_64 classifier" >&2; exit 1;
}
native_bundle="${native_bundles[0]}"
mkdir "$native_stage/extracted"
(
    cd "$native_stage/extracted"
    jar --extract --file "$native_bundle" META-INF/native/linux/x86_64/libpangrova_native.so
)
native_library="$native_stage/extracted/META-INF/native/linux/x86_64/libpangrova_native.so"
[[ -f "$native_library" ]] || { echo "Production classifier has no canonical JNI" >&2; exit 1; }
bash "$project_root/pangrova-worker-native/bin/package-native-lib.sh" --production \
    "$native_library" >&2
(
    cd "$native_stage/lib"
    sha256sum -- *.jar
) > "$native_stage/runtime.sha256"
native_sha="$(sha256sum "$native_library" | cut -d " " -f 1)"
bundle_sha="$(sha256sum "$native_bundle" | cut -d " " -f 1)"
manifest_sha="$(sha256sum "$native_stage/runtime.sha256" | cut -d " " -f 1)"
printf 'image.id=%s\nsource.commit=%s\nclassifier=linux-x86_64\nnative.sha256=%s\nbundle.sha256=%s\nmanifest.sha256=%s\n' \
    "$image_id" "$source_commit" "$native_sha" "$bundle_sha" "$manifest_sha" \
    > "$native_stage/image-provenance.properties"
chmod 600 "$native_stage/image-provenance.properties"
[[ "$(docker image inspect --format '{{.Id}}' "$image_id")" == "$image_id" ]] || {
    echo "Production image ID changed during staging" >&2; exit 1;
}
printf '%s\n' "$native_bundle"
