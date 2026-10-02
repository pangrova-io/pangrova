#!/usr/bin/env bash
# Build the ASan-specific dependencies required by FindVelox.cmake.
#
# The regular native bootstrap intentionally produces Release artifacts.  ASan
# must not reuse its Velox, Protobuf, or Abseil archives: mixing instrumented
# inline/template code with Release archives creates false reports and leaves
# real dependency accesses invisible.  This script reuses the already-fetched
# sources and Velox third-party install, but writes every ASan artifact to a
# separate prefix/build tree.
set -euo pipefail

PANGROVA_HOME="${PANGROVA_HOME:-$HOME/.pangrova}"
VELOX_HOME="${VELOX_HOME:-$PANGROVA_HOME/velox}"
WORK_DIR="${PANGROVA_NATIVE_BUILD_DIR:-${TMPDIR:-/tmp}/pangrova-native-build}"
ASAN_PREFIX="${PANGROVA_ASAN_DEPS_PREFIX:-$PANGROVA_HOME/native-deps-asan}"
ASAN_VELOX_BUILD_DIR="${PANGROVA_ASAN_VELOX_BUILD_DIR:-$VELOX_HOME/_build/asan-ci}"
NPROC="${PANGROVA_ASAN_BUILD_JOBS:-2}"

ABSEIL_SOURCE="$WORK_DIR/abseil-cpp"
PROTOBUF_SOURCE="$WORK_DIR/protobuf"
ASAN_FLAGS="-fsanitize=address -fno-omit-frame-pointer"

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

command -v cmake >/dev/null 2>&1 || die "cmake not found"
command -v ninja >/dev/null 2>&1 || die "ninja not found"
[[ -f "$ABSEIL_SOURCE/CMakeLists.txt" ]] || \
  die "Abseil source missing; run setup-native-build-env.sh first"
[[ -f "$PROTOBUF_SOURCE/CMakeLists.txt" ]] || \
  die "Protobuf source missing; run setup-native-build-env.sh first"
[[ -f "$VELOX_HOME/CMakeLists.txt" ]] || \
  die "Velox source missing; run setup-native-build-env.sh first"
bash "$(dirname "${BASH_SOURCE[0]}")/apply-velox-patches.sh" apply "$VELOX_HOME"

mkdir -p "$ASAN_PREFIX"

if [[ ! -f "$ASAN_PREFIX/lib/cmake/absl/abslConfig.cmake" ]]; then
  cmake -S "$ABSEIL_SOURCE" -B "$ABSEIL_SOURCE/build-asan-ci" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_INSTALL_PREFIX="$ASAN_PREFIX" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_BUILD_TESTING=OFF
  cmake --build "$ABSEIL_SOURCE/build-asan-ci" --target install \
    --parallel "$NPROC"
fi

if [[ ! -f "$ASAN_PREFIX/lib/libprotobuf.a" ]]; then
  cmake -S "$PROTOBUF_SOURCE" -B "$PROTOBUF_SOURCE/build-asan-ci" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_INSTALL_PREFIX="$ASAN_PREFIX" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_PREFIX_PATH="$ASAN_PREFIX" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -Dabsl_DIR="$ASAN_PREFIX/lib/cmake/absl" \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_EXAMPLES=OFF \
    -Dprotobuf_ABSL_PROVIDER=package \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF
  cmake --build "$PROTOBUF_SOURCE/build-asan-ci" --target install \
    --parallel "$NPROC"
fi

# The hosted Linux runner cannot hold both complete Release and ASan Velox
# object trees.  Release setup has already installed the shared/native
# dependencies needed below, so CI may reclaim only that explicit build tree
# before compiling ASan.  This remains opt-in for developer machines.
if [[ "${PANGROVA_ASAN_CI_PRUNE_RELEASE:-0}" == "1" ]]; then
  release_build="$VELOX_HOME/_build/release"
  if [[ -d "$release_build" && "$release_build" != "$ASAN_VELOX_BUILD_DIR" ]]; then
    rm -rf "$release_build"
  fi
fi

asan_velox_ready=false
if [[ -f "$ASAN_VELOX_BUILD_DIR/lib/libvelox.a" &&
      -f "$ASAN_VELOX_BUILD_DIR/CMakeCache.txt" ]] &&
    grep -Eq '^CMAKE_CXX_FLAGS:.*-fsanitize=address' \
      "$ASAN_VELOX_BUILD_DIR/CMakeCache.txt"; then
  asan_velox_ready=true
fi

if [[ "$asan_velox_ready" != true ]]; then
  # Release + -O1/-g1 keeps the monolithic archive small enough for a hosted
  # runner while retaining symbolized ASan stacks.  Full -g makes libvelox.a
  # several gigabytes and exhausts the runner before Pangrova can link.
  cmake -S "$VELOX_HOME" -B "$ASAN_VELOX_BUILD_DIR" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_C_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_EXE_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_PREFIX_PATH="$ASAN_PREFIX;$VELOX_HOME/deps-install" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -Dabsl_DIR="$ASAN_PREFIX/lib/cmake/absl" \
    -DProtobuf_DIR="$ASAN_PREFIX/lib/cmake/protobuf" \
    -DVELOX_MONO_LIBRARY=ON \
    -DVELOX_BUILD_TESTING=OFF \
    -DVELOX_BUILD_TEST_UTILS=OFF \
    -DVELOX_ENABLE_TESTING=OFF \
    -DVELOX_ENABLE_BENCHMARKS=OFF \
    -DVELOX_ENABLE_EXAMPLES=OFF \
    -DVELOX_ENABLE_SUBSTRAIT=ON \
    -DVELOX_ENABLE_ARROW=ON \
    -DVELOX_ENABLE_PARQUET=ON \
    -DVELOX_ENABLE_HIVE_CONNECTOR=ON \
    -DVELOX_ENABLE_PRESTO_FUNCTIONS=ON \
    -DVELOX_ENABLE_SPARK_FUNCTIONS=ON \
    -DVELOX_ENABLE_AGGREGATES=ON \
    -DVELOX_ENABLE_S3=ON \
    -DVELOX_ENABLE_HDFS=OFF \
    -DVELOX_ENABLE_GCS=OFF \
    -DVELOX_ENABLE_GEO=OFF \
    -DVELOX_ENABLE_TPCH_CONNECTOR=OFF \
    -DVELOX_ENABLE_TPCDS_CONNECTOR=OFF \
    -DVELOX_ENABLE_REMOTE_FUNCTIONS=OFF
fi

# A cached archive must still incorporate the reviewed source patch.
cmake --build "$ASAN_VELOX_BUILD_DIR" --target velox --parallel "$NPROC"
bash "$(dirname "${BASH_SOURCE[0]}")/apply-velox-patches.sh" record "$VELOX_HOME" \
  "$ASAN_VELOX_BUILD_DIR/lib/libvelox.a"

[[ -f "$ASAN_VELOX_BUILD_DIR/lib/libvelox.a" ]] || \
  die "ASan Velox archive was not produced"
grep -Eq '^CMAKE_CXX_FLAGS:.*-fsanitize=address' \
  "$ASAN_VELOX_BUILD_DIR/CMakeCache.txt" || \
  die "ASan Velox cache does not contain address-sanitizer flags"

cat > "$PANGROVA_HOME/asan-env.sh" <<EOF
export VELOX_BUILD_DIR="$ASAN_VELOX_BUILD_DIR"
export PANGROVA_ASAN_PROTOBUF_LIB="$ASAN_PREFIX/lib/libprotobuf.a"
export PANGROVA_ASAN_ABSL_DIR="$ASAN_PREFIX/lib/cmake/absl"
EOF

printf 'ASan dependencies ready: %s\n' "$ASAN_VELOX_BUILD_DIR"
