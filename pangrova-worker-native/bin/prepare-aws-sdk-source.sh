#!/usr/bin/env bash
# Materialize the exact aws-sdk-cpp submodule tree pinned by Velox. GitHub's
# large-repository clone endpoint is unreliable in the production Docker build;
# codeload serves the same immutable commits without Git pack negotiation.
set -euo pipefail

velox_dir="${1:?Usage: prepare-aws-sdk-source.sh VELOX_DIR}"
version_file="$velox_dir/scripts/setup-versions.sh"
grep -Fxq 'AWS_SDK_VERSION="1.11.654"' "$version_file" || {
  echo "Unsupported Velox AWS SDK version in $version_file" >&2
  exit 1
}

dependency_dir="$velox_dir/deps-download"
source_dir="$dependency_dir/aws-sdk-cpp"
if [[ -d "$source_dir" ]]; then
  echo "AWS SDK source already exists at $source_dir" >&2
  exit 1
fi
mkdir -p "$dependency_dir"
staging="$(mktemp -d "$dependency_dir/.aws-sdk-cpp.XXXXXX")"
trap 'rm -rf "$staging"' EXIT

extract_archive() {
  local repository="$1" revision="$2" destination="$3"
  local archive="$staging/archive.tar.gz"
  mkdir -p "$destination"
  curl -fsSL --connect-timeout 30 --max-time 600 \
    "https://codeload.github.com/$repository/tar.gz/$revision" -o "$archive"
  gzip -t "$archive"
  tar -xzf "$archive" --strip-components=1 -C "$destination"
  rm "$archive"
}

# aws-sdk-cpp tag 1.11.654 resolves to this commit.
extract_archive aws/aws-sdk-cpp 14b33e1dadc982e21b40c2bae1b90cc0b2c99a5d \
  "$staging/source"
extract_archive awslabs/aws-crt-cpp fbe8fa52486c0a7a3aa8b1998a5b175d15ad2cac \
  "$staging/source/crt/aws-crt-cpp"

# Gitlinks from aws-crt-cpp@fbe8fa52486c0a7a3aa8b1998a5b175d15ad2cac.
# Keep these exact commits when the parent AWS SDK version changes.
while read -r repository revision; do
  extract_archive "awslabs/$repository" "$revision" \
    "$staging/source/crt/aws-crt-cpp/crt/$repository"
done <<'SUBMODULES'
aws-c-auth ab03bdd996437d9097953ebb9495de71b6adc537
aws-c-cal 8703b3e5930c9fd508025b268ab837fc9df3c4fd
aws-c-common 2b67a658e461520f1de20d64342b91ddcedc7ebb
aws-c-compression f951ab2b819fc6993b6e5e6cfef64b1a1554bfc8
aws-c-event-stream 2a0f7c9fe656c4789fa762a4ff04d06401abd282
aws-c-http ce0d65623bff28f03204756d9d1b3366bd0b387d
aws-c-io 300e8b46aab672172f8d9303e1b9eeeffcb77713
aws-c-mqtt 1d512d92709f60b74e2cafa018e69a2e647f28e9
aws-c-s3 3afa5d08be95e82199a153e3abbe59bbb42638d7
aws-c-sdkutils f678bda9e21f7217e4bbf35e0d1ea59540687933
aws-checksums 9978ba2c33a7a259c1a6bd0f62abe26827d03b85
aws-lc 98500e8bc7dc3e3e5871519abf12cdc781ebe4e1
s2n a7862238a9d6703e0a3e2d7ea4341de8d57429bd
SUBMODULES

test -f "$staging/source/crt/aws-crt-cpp/crt/aws-c-common/CMakeLists.txt"
mv "$staging/source" "$source_dir"
