#!/usr/bin/env bash
# Install AWS C++ SDK for Velox S3 connector support.
# Version must match Velox's scripts/setup-versions.sh: AWS_SDK_VERSION="1.11.654"
# Components: s3, identity-management (matches Velox's install_aws_deps)
#
# Usage: ./install-aws-sdk.sh [install_prefix]
#   install_prefix defaults to ~/.pangrova/native-deps

set -euo pipefail

AWS_SDK_VERSION="1.11.654"
INSTALL_PREFIX="${1:-$HOME/.pangrova/native-deps}"
# Fall back to /tmp rather than the current directory: TMPDIR is unset on most
# Linux CI runners, and a relative BUILD_DIR would put the dependency build tree
# inside the repository and hand a relative path to the `rm -rf` trap below.
BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pangrova-aws-sdk-build.XXXXXX")"
JOBS="${JOBS:-$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)}"
trap 'rm -rf "${BUILD_DIR}"' EXIT

echo "=== AWS C++ SDK ${AWS_SDK_VERSION} ==="
echo "  Install prefix: ${INSTALL_PREFIX}"
echo "  Build dir:      ${BUILD_DIR}"
echo "  Parallel jobs:  ${JOBS}"
echo ""

# Clone
echo "[1/4] Cloning aws-sdk-cpp ${AWS_SDK_VERSION}..."
git clone --depth 1 --recurse-submodules --branch "${AWS_SDK_VERSION}" \
  https://github.com/aws/aws-sdk-cpp.git "${BUILD_DIR}/src"

# Configure
echo "[2/4] Configuring..."
cmake -S "${BUILD_DIR}/src" -B "${BUILD_DIR}/build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
  -DCMAKE_PREFIX_PATH="${INSTALL_PREFIX}" \
  -DBUILD_SHARED_LIBS=OFF \
  -DMINIMIZE_SIZE=ON \
  -DENABLE_TESTING=OFF \
  -DBUILD_ONLY="s3;identity-management;sts" \
  -DCMAKE_INSTALL_LIBDIR=lib

# Build
echo "[3/4] Building (${JOBS} jobs)..."
cmake --build "${BUILD_DIR}/build" -j "${JOBS}"

# Install
echo "[4/4] Installing to ${INSTALL_PREFIX}..."
cmake --install "${BUILD_DIR}/build"

echo ""
echo "=== Done! AWS C++ SDK installed to ${INSTALL_PREFIX} ==="
echo "  Libraries: ${INSTALL_PREFIX}/lib/libaws-cpp-sdk-*.a"
echo "  Headers:   ${INSTALL_PREFIX}/include/aws/"
