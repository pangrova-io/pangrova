#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TEST_TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEST_TMP_DIR}"' EXIT

EMPTY_MAVEN_REPOSITORY="${TEST_TMP_DIR}/repository"
MAVEN_REPO_LOCAL="${EMPTY_MAVEN_REPOSITORY}" \
    "${ROOT_DIR}/.pangrova/scripts/proto-descriptor-diff.sh"

PROTOBUF_VERSION=$(sed -n \
    's:.*<protobuf.version>\([^<]*\)</protobuf.version>.*:\1:p' \
    "${ROOT_DIR}/pom.xml" | head -1)
test -f "${EMPTY_MAVEN_REPOSITORY}/com/google/protobuf/protobuf-java/${PROTOBUF_VERSION}/protobuf-java-${PROTOBUF_VERSION}.jar"
find "${EMPTY_MAVEN_REPOSITORY}/com/google/protobuf/protoc/${PROTOBUF_VERSION}" \
    -type f -name "protoc-${PROTOBUF_VERSION}-*.exe" -perm -u+x -print -quit \
    | grep -q .

echo "OK: descriptor gate bootstraps and runs from an empty Maven repository."
