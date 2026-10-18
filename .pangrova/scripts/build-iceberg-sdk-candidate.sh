#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="6976e020b894f6a6777704df2b8c4458cb291ae9"
source_sha256="9a79d359e8856290edb99b227d469ee4e7e4d1d92ac81e8488f270adca9c415f"
patch_sha256="b5331c0c7404df2fc2b86e14b59c0c881fe5d32337f6fa53c6a3e50699ed0423"
candidate_version="1.11.0-pangrova-schema-candidate1"
gradle_bin="${GRADLE_BIN:-gradle}"
patch_file="$repo_root/pangrova-connector/pangrova-connector-iceberg/patches/iceberg-jdbc-schema-isolation.patch"

if [[ -z "${JAVA_HOME:-}" ]] || [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "JAVA_HOME must point to JDK 17" >&2
  exit 2
fi
java_major="$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ { split($2, v, "."); print v[1] }')"
if [[ "$java_major" != "17" ]]; then
  echo "JDK 17 is required to build the Iceberg SDK candidate" >&2
  exit 2
fi
if ! command -v "$gradle_bin" >/dev/null 2>&1; then
  echo "Gradle 8.14.4 is required; set GRADLE_BIN to its executable" >&2
  exit 2
fi
gradle_version="$("$gradle_bin" --version | awk '/^Gradle / { print $2; exit }')"
if [[ "$gradle_version" != "8.14.4" ]]; then
  echo "Gradle 8.14.4 is required, found ${gradle_version:-unknown}" >&2
  exit 2
fi

actual_patch_sha256="$(shasum -a 256 "$patch_file" | awk '{print $1}')"
if [[ "$actual_patch_sha256" != "$patch_sha256" ]]; then
  echo "Iceberg patch checksum mismatch: $actual_patch_sha256" >&2
  exit 1
fi

build_root="$(mktemp -d "${TMPDIR:-/tmp}/pangrova-iceberg-sdk.XXXXXX")"
trap 'rm -rf "$build_root"' EXIT
archive="$build_root/iceberg.tar.gz"
archive_url="https://codeload.github.com/apache/iceberg/tar.gz/$source_commit"
curl --fail --location --silent --show-error "$archive_url" --output "$archive"
actual_source_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
if [[ "$actual_source_sha256" != "$source_sha256" ]]; then
  echo "Iceberg source archive checksum mismatch: $actual_source_sha256" >&2
  exit 1
fi

tar -xzf "$archive" -C "$build_root"
source_root="$build_root/iceberg-$source_commit"
patch -p1 -d "$source_root" < "$patch_file"
printf '%s\n' "$candidate_version" > "$source_root/version.txt"
printf 'source_commit=%s\nsource_sha256=%s\npatch_sha256=%s\nversion=%s\n' \
  "$source_commit" "$source_sha256" "$patch_sha256" "$candidate_version" \
  > "$source_root/iceberg-build.properties"

cd "$source_root"
"$gradle_bin" --no-daemon \
  :iceberg-api:publishToMavenLocal \
  :iceberg-bundled-guava:publishToMavenLocal \
  :iceberg-common:publishToMavenLocal \
  :iceberg-core:publishToMavenLocal \
  :iceberg-orc:publishToMavenLocal \
  :iceberg-parquet:publishToMavenLocal

echo "Published $candidate_version from $source_commit with patch $patch_sha256"
