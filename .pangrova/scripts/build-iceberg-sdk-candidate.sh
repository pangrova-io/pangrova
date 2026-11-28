#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="6976e020b894f6a6777704df2b8c4458cb291ae9"
source_sha256="9a79d359e8856290edb99b227d469ee4e7e4d1d92ac81e8488f270adca9c415f"
patch_sha256="b5331c0c7404df2fc2b86e14b59c0c881fe5d32337f6fa53c6a3e50699ed0423"
fixed_patch_sha256="f3dfc99a1233a4092b3b79413a23660da6510c04218efae6ae6513ae09ba8243"
candidate_version="1.11.0-pangrova-schema-candidate4"
gradle_bin="${GRADLE_BIN:-gradle}"
patch_file="$repo_root/pangrova-connector/pangrova-connector-iceberg/patches/iceberg-jdbc-schema-isolation.patch"

fixed_patch_file="$repo_root/pangrova-connector/pangrova-connector-iceberg/patches/iceberg-avro-fixed-buffer.patch"

if [[ -z "${JAVA_HOME:-}" ]] || [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "JAVA_HOME must point to JDK 17" >&2
  exit 2
fi
java_major="$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/^(openjdk|java) version "/ { split($2, v, "."); print v[1]; exit }')"
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

actual_fixed_patch_sha256="$(shasum -a 256 "$fixed_patch_file" | awk '{print $1}')"
if [[ "$actual_fixed_patch_sha256" != "$fixed_patch_sha256" ]]; then
  echo "Iceberg fixed-buffer patch checksum mismatch: $actual_fixed_patch_sha256" >&2
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
patch -p1 -d "$source_root" < "$fixed_patch_file"
printf '%s\n' "$candidate_version" > "$source_root/version.txt"
printf 'source_commit=%s\nsource_sha256=%s\npatch_sha256=%s\nfixed_patch_sha256=%s\nversion=%s\n' \
  "$source_commit" "$source_sha256" "$patch_sha256" "$fixed_patch_sha256" "$candidate_version" \
  > "$source_root/iceberg-build.properties"

cd "$source_root"
# These are the pinned Gradle 8.14.4 dependency repository resolver's
# internal retry settings (revalidate on Gradle upgrades). Keep three
# attempts, but allow transient rate limits time to recover. Backoff is
# per repository operation, not an overall build timeout or Retry-After
# guarantee. Persistent failures still fail the original Gradle command.
gradle_repository_args=(
  -Dorg.gradle.internal.repository.max.tentatives=3
  -Dorg.gradle.internal.repository.initial.backoff=30000
)
"$gradle_bin" "${gradle_repository_args[@]}" --no-daemon --max-workers=2 \
  :iceberg-core:test --tests org.apache.iceberg.avro.TestFixedByteBufferWriter
publish_args=(--no-daemon)
if [[ -n "${PANGROVA_ICEBERG_MAVEN_REPOSITORY:-}" ]]; then
  publish_args+=("-Dmaven.repo.local=$PANGROVA_ICEBERG_MAVEN_REPOSITORY")
fi
"$gradle_bin" "${gradle_repository_args[@]}" "${publish_args[@]}" \
  :iceberg-api:publishToMavenLocal \
  :iceberg-bundled-guava:publishToMavenLocal \
  :iceberg-common:publishToMavenLocal \
  :iceberg-core:publishToMavenLocal \
  :iceberg-orc:publishToMavenLocal \
  :iceberg-parquet:publishToMavenLocal

if [[ -n "${PANGROVA_ICEBERG_MAVEN_REPOSITORY:-}" ]]; then
  for module in api bundled-guava common core orc parquet; do
    artifact="$PANGROVA_ICEBERG_MAVEN_REPOSITORY/org/apache/iceberg/iceberg-$module/$candidate_version/iceberg-$module-$candidate_version"
    for extension in jar pom; do
      if [[ ! -s "$artifact.$extension" ]]; then
        echo "Missing Iceberg SDK publication: $artifact.$extension" >&2
        exit 1
      fi
    done
  done
fi

echo "Published $candidate_version from $source_commit with patches $patch_sha256 and $fixed_patch_sha256"
