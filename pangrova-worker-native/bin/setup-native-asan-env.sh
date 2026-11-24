#!/usr/bin/env bash
# Build the ASan-specific dependencies required by FindVelox.cmake.
#
# The regular native bootstrap intentionally produces Release artifacts.  ASan
# must not reuse its Velox, Protobuf, Abseil, RE2, gRPC or Flight archives: mixing instrumented
# inline/template code with Release archives creates false reports and leaves
# real dependency accesses invisible.  This script reuses the already-fetched
# sources and Velox third-party install, but writes every ASan artifact to a
# separate prefix/build tree.
set -euo pipefail
ASAN_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PANGROVA_HOME="${PANGROVA_HOME:-$HOME/.pangrova}"
if [[ "$(uname -s)" == "Darwin" ]]; then
  if [[ -x "${PANGROVA_BREW:-$PANGROVA_HOME/homebrew/bin/brew}" ]]; then
    PANGROVA_BREW="${PANGROVA_BREW:-$PANGROVA_HOME/homebrew/bin/brew}"
  else
    PANGROVA_BREW="$(command -v brew || true)"
  fi
  HOMEBREW_PREFIX="$("$PANGROVA_BREW" --prefix 2>/dev/null || true)"
  source "$ASAN_SCRIPT_DIR/resolve-darwin-openssl.sh"
  configure_darwin_openssl "$PANGROVA_BREW"
  BISON_PREFIX="$("$PANGROVA_BREW" --prefix bison 2>/dev/null || true)"
  FLEX_PREFIX="$("$PANGROVA_BREW" --prefix flex 2>/dev/null || true)"
fi
VELOX_HOME="${VELOX_HOME:-$PANGROVA_HOME/velox}"
WORK_DIR="${PANGROVA_NATIVE_BUILD_DIR:-${TMPDIR:-/tmp}/pangrova-native-build}"
ASAN_PREFIX="${PANGROVA_ASAN_DEPS_PREFIX:-$PANGROVA_HOME/native-deps-asan}"
ASAN_VELOX_BUILD_DIR="${PANGROVA_ASAN_VELOX_BUILD_DIR:-$VELOX_HOME/_build/asan-ci}"
if [[ -n "${PANGROVA_ASAN_BUILD_JOBS:-}" ]]; then
  NPROC="$PANGROVA_ASAN_BUILD_JOBS"
else
  NPROC=2
  if [[ "$(uname -s)" == "Darwin" ]]; then
    memory_gb=$(( $(sysctl -n hw.memsize) / 1073741824 ))
  else
    memory_gb=$(awk '/^MemTotal:/ {print int($2 / 1048576)}' /proc/meminfo 2>/dev/null || echo 0)
  fi
  if [[ "$memory_gb" -gt 0 && $(( memory_gb / 6 )) -lt "$NPROC" ]]; then
    NPROC=$(( memory_gb / 6 ))
    [[ "$NPROC" -lt 1 ]] && NPROC=1
  fi
fi
# ExternalProject CMake/Make builds do not inherit the outer Ninja job limit.
export CMAKE_BUILD_PARALLEL_LEVEL="$NPROC"
export NUM_THREADS="$NPROC"

ABSEIL_SOURCE="$WORK_DIR/abseil-cpp"
PROTOBUF_SOURCE="$WORK_DIR/protobuf"
RE2_SOURCE="$WORK_DIR/re2"
GRPC_SOURCE="$WORK_DIR/grpc"
FOLLY_SOURCE="$VELOX_HOME/deps-download/folly"
ASAN_FLAGS="-fsanitize=address -fno-omit-frame-pointer"

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

CHECK_TOOLS=0
case "${1:-}" in
  "") ;;
  --check-tools) CHECK_TOOLS=1 ;;
  *) printf 'Usage: %s [--check-tools]\n' "$0" >&2; exit 2 ;;
esac
[[ "$#" -le 1 ]] || { printf 'Usage: %s [--check-tools]\n' "$0" >&2; exit 2; }

require_source_revision() {
  local source="$1" revision="$2" actual expected
  actual="$(git -C "$source" rev-parse --verify HEAD)" || die "Dependency checkout missing: $source"
  expected="$(git -C "$source" rev-parse --verify "$revision^{commit}")" \
    || die "Pinned dependency revision missing: $revision"
  [[ "$actual" == "$expected" ]] || die "Dependency checkout differs from $revision: $source"
}

command -v cmake >/dev/null 2>&1 || die "cmake not found"
CMAKE_LAUNCHER_FLAGS=""
if command -v ccache >/dev/null 2>&1; then
  export CCACHE_DIR="${CCACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/ccache}"
  export CCACHE_COMPRESS=1
  export CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-3G}"
  mkdir -p "$CCACHE_DIR"
  CMAKE_LAUNCHER_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
fi
# Reject a package-manager cmake shim that has no Modules directory, and keep
# the working binary ahead of anything the Velox dependency setup adds.
PANGROVA_CMAKE_BIN="$(command -v cmake)"
"$PANGROVA_CMAKE_BIN" --version >/dev/null 2>&1 \
  || die "cmake at $PANGROVA_CMAKE_BIN cannot locate its modules"
export PATH="$(dirname "$PANGROVA_CMAKE_BIN"):$PATH"

# Use the same LZ4 archive as the Release Flight build and keep Arrow's hash check.
export ARROW_LZ4_URL="${ARROW_LZ4_URL:-https://codeload.github.com/lz4/lz4/tar.gz/refs/tags/v1.10.0}"
command -v ninja >/dev/null 2>&1 || die "ninja not found"
command -v patch >/dev/null 2>&1 || die "patch not found"
[[ -f "$ABSEIL_SOURCE/CMakeLists.txt" ]] || \
  die "Abseil source missing; run setup-native-build-env.sh first"
[[ -f "$PROTOBUF_SOURCE/CMakeLists.txt" ]] || \
  die "Protobuf source missing; run setup-native-build-env.sh first"
[[ -f "$RE2_SOURCE/CMakeLists.txt" && -f "$GRPC_SOURCE/CMakeLists.txt" ]] || \
  die "RE2/gRPC source missing; run setup-native-build-env.sh first"
[[ -f "$VELOX_HOME/CMakeLists.txt" ]] || \
  die "Velox source missing; run setup-native-build-env.sh first"
if [[ -z "${PANGROVA_ASAN_FOLLY_PREFIX:-}" ]]; then
  [[ -f "$FOLLY_SOURCE/CMakeLists.txt" ]] || \
    die "Folly source missing; run setup-native-build-env.sh first"
fi
require_source_revision "$ABSEIL_SOURCE" "${ABSEIL_VERSION:-20240116.2}"
require_source_revision "$PROTOBUF_SOURCE" "${PROTOBUF_VERSION:-v25.3}"
require_source_revision "$RE2_SOURCE" "${RE2_VERSION:-2024-07-02}"
require_source_revision "$GRPC_SOURCE" "${GRPC_VERSION:-v1.65.0}"
if [[ "$CHECK_TOOLS" == 1 ]]; then
  printf 'ASan dependency toolchain and sources verified\n'
  exit 0
fi
libevent_prefix=""
if [[ "$(uname -s)" == "Darwin" && -z "${PANGROVA_ASAN_FOLLY_PREFIX:-}" ]]; then
  source "$ASAN_SCRIPT_DIR/resolve-darwin-libevent.sh"
  libevent_prefix="$(resolve_darwin_libevent "$PANGROVA_BREW")"
fi

bash "$(dirname "${BASH_SOURCE[0]}")/apply-velox-patches.sh" apply "$VELOX_HOME"
arrow_build_patch="$ASAN_SCRIPT_DIR/../patches/velox-arrow-build-flags.patch"
if ! git -C "$VELOX_HOME" apply --reverse --check "$arrow_build_patch" >/dev/null 2>&1; then
  git -C "$VELOX_HOME" apply --check "$arrow_build_patch"
  git -C "$VELOX_HOME" apply "$arrow_build_patch"
fi

mkdir -p "$ASAN_PREFIX"

build_asan_dependency() {
  local source="$1" build="$2"
  shift 2
  # Reconfigure and incrementally build the actual source every time. Package
  # presence alone does not bind cached objects to a compiler or sanitizer ABI.
  cmake -S "$source" -B "$build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_C_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_EXE_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_INSTALL_PREFIX="$ASAN_PREFIX" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_PREFIX_PATH="$ASAN_PREFIX;$VELOX_HOME/deps-install" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "$@"
  cmake --build "$build" --target install --parallel "$NPROC"
}

build_asan_dependency "$ABSEIL_SOURCE" "$ABSEIL_SOURCE/build-asan-ci" \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_BUILD_TESTING=OFF

build_asan_dependency "$PROTOBUF_SOURCE" "$PROTOBUF_SOURCE/build-asan-ci" \
    -Dabsl_DIR:PATH="$ASAN_PREFIX/lib/cmake/absl" \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_EXAMPLES=OFF \
    -Dprotobuf_ABSL_PROVIDER=package \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF

build_asan_dependency "$RE2_SOURCE" "$RE2_SOURCE/build-asan-ci" \
    -Dabsl_DIR:PATH="$ASAN_PREFIX/lib/cmake/absl" \
    -DBUILD_SHARED_LIBS=OFF -DRE2_BUILD_TESTING=OFF

asan_ssl_args=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  asan_ssl_args+=("-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX")
fi
build_asan_dependency "$GRPC_SOURCE" "$GRPC_SOURCE/build-asan-ci" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -Dabsl_DIR:PATH="$ASAN_PREFIX/lib/cmake/absl" \
    -DProtobuf_DIR:PATH="$ASAN_PREFIX/lib/cmake/protobuf" \
    -Dre2_DIR:PATH="$ASAN_PREFIX/lib/cmake/re2" \
    -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_BUILD_CSHARP_EXT=OFF \
    -DBUILD_SHARED_LIBS=OFF \
    -DgRPC_ABSL_PROVIDER=package -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package -DgRPC_CARES_PROVIDER=module \
    -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package \
    ${asan_ssl_args[@]+"${asan_ssl_args[@]}"}

build_asan_folly() {
  if [[ -z "${PANGROVA_ASAN_FOLLY_PREFIX:-}" ]]; then
    local folly_patch
    for folly_patch in \
      "$ASAN_SCRIPT_DIR/../patches/folly-2026.01.05-openssl-asn1.patch" \
      "$ASAN_SCRIPT_DIR/../patches/folly-2026.01.05-socket-function-types.patch" \
      "$ASAN_SCRIPT_DIR/../patches/folly-2026.01.05-spookyhash-alignment.patch"; do
      if ! patch --dry-run --batch --forward --reverse -d "$FOLLY_SOURCE" -p1 \
          -i "$folly_patch" >/dev/null 2>&1; then
        patch --dry-run --batch --forward -d "$FOLLY_SOURCE" -p1 -i "$folly_patch"
        patch --batch --forward -d "$FOLLY_SOURCE" -p1 -i "$folly_patch"
      fi
    done
    local architecture_flags
    architecture_flags="$(source "$VELOX_HOME/scripts/setup-helper-functions.sh"; get_cxx_flags)"
    build_asan_dependency "$FOLLY_SOURCE" "$WORK_DIR/folly-asan-build" \
      -DCMAKE_CXX_STANDARD=20 \
      "-DCMAKE_CXX_FLAGS=$ASAN_FLAGS $architecture_flags" \
      "-DCMAKE_PREFIX_PATH=$ASAN_PREFIX;$VELOX_HOME/deps-install${libevent_prefix:+;$libevent_prefix}${HOMEBREW_PREFIX:+;$HOMEBREW_PREFIX}" \
      -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DFOLLY_LIBRARY_SANITIZE_ADDRESS=ON -DFOLLY_HAVE_INT128_T=ON \
      -DBUILD_SHARED_LIBS=OFF -DBUILD_TESTS=OFF -DGFLAGS_SHARED=FALSE \
      "-DCMAKE_PROJECT_folly_INCLUDE=$ASAN_SCRIPT_DIR/../cmake/AsanFollyCompat.cmake" \
      ${asan_ssl_args[@]+"${asan_ssl_args[@]}"}
    PANGROVA_ASAN_FOLLY_PREFIX="$ASAN_PREFIX"
  fi
  [[ -f "$PANGROVA_ASAN_FOLLY_PREFIX/lib/cmake/folly/folly-config.cmake" ]] || \
    die "ASan Folly package is missing: $PANGROVA_ASAN_FOLLY_PREFIX"
}

build_asan_folly

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

configure_asan_velox() {
  # Use the same platform dependency locations as the Release bootstrap, but
  # never add its native-deps prefix: Protobuf and Abseil must remain ASan-only.
  velox_prefix_path="$ASAN_PREFIX;$VELOX_HOME/deps-install"
  velox_platform_args=()
  local folly_config="$PANGROVA_ASAN_FOLLY_PREFIX/lib/cmake/folly"
  velox_platform_args+=(-Dfolly_SOURCE=SYSTEM "-Dfolly_DIR:PATH=$folly_config")
  if [[ "$(uname -s)" == "Darwin" ]]; then
    [[ -n "$HOMEBREW_PREFIX" ]] && velox_prefix_path+=";$HOMEBREW_PREFIX"
    velox_platform_args+=("-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX")
    [[ -n "$BISON_PREFIX" ]] && velox_platform_args+=("-DBISON_EXECUTABLE=$BISON_PREFIX/bin/bison")
    [[ -n "$FLEX_PREFIX" ]] && velox_platform_args+=("-DFLEX_EXECUTABLE=$FLEX_PREFIX/bin/flex")
    [[ -n "$FLEX_PREFIX" ]] && velox_platform_args+=("-DFLEX_INCLUDE_DIR=$FLEX_PREFIX/include")
  fi
  # Release + -O1/-g1 keeps the monolithic archive small enough for a hosted
  # runner while retaining symbolized ASan stacks.  Full -g makes libvelox.a
  # several gigabytes and exhausts the runner before Pangrova can link.
  # Velox gives the environment selector precedence over CMake cache variables.
  # Use the bundled resolver so the matching Arrow patch target exists.
  Arrow_SOURCE=BUNDLED cmake -S "$VELOX_HOME" -B "$ASAN_VELOX_BUILD_DIR" -GNinja $CMAKE_LAUNCHER_FLAGS \
    -DPANGROVA_FUNCTION_SELECTION_TOOLS="$ASAN_SCRIPT_DIR/../../pangrova-protocol/tools/function-contracts" \
    -DPANGROVA_FUNCTION_SELECTION_DECLARATION="$ASAN_SCRIPT_DIR/../../pangrova-protocol/src/main/resources/function-contracts/execution-selection.json" \
    -DPANGROVA_FUNCTION_SDK_PATCH_FILE="$ASAN_SCRIPT_DIR/../patches/velox-function-metadata-snapshot.patch" \
    -DPython3_EXECUTABLE="${PANGROVA_FUNCTION_CONTRACT_PYTHON:-python3}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_C_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_EXE_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_SHARED_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_PREFIX_PATH="$velox_prefix_path" \
    ${velox_platform_args[@]+"${velox_platform_args[@]}"} \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POLICY_DEFAULT_CMP0167=NEW \
    -Dsimdjson_SOURCE=BUNDLED \
    -Dabsl_DIR:PATH="$ASAN_PREFIX/lib/cmake/absl" \
    -DProtobuf_DIR="$ASAN_PREFIX/lib/cmake/protobuf" \
    -Dre2_DIR:PATH="$ASAN_PREFIX/lib/cmake/re2" \
    -DVELOX_MONO_LIBRARY=ON \
    -DVELOX_BUILD_TESTING=OFF \
    -DVELOX_BUILD_TEST_UTILS=OFF \
    -DVELOX_ENABLE_TESTING=OFF \
    -DVELOX_ENABLE_BENCHMARKS=OFF \
    -DVELOX_ENABLE_EXAMPLES=OFF \
    -DVELOX_ENABLE_SUBSTRAIT=ON \
    -DVELOX_ENABLE_ARROW=ON \
    "-DCMAKE_PROJECT_Arrow_INCLUDE:FILEPATH=$ASAN_SCRIPT_DIR/../cmake/AsanArrowSource.cmake" \
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
}

# Reconfigure even an existing ASan tree: flags alone do not prove that its
# cached Protobuf and Abseil paths belong to the requested dependency cohort.
configure_asan_velox

# Use the matching SDK's authoritative Arrow external-project source, not a
# guessed system source or the Release tree which CI may already have retired.
# Building libvelox does not fetch Arrow (it is not linked into the archive), so
# materialize and patch the ExternalProject source before consuming it.
cmake --build "$ASAN_VELOX_BUILD_DIR" --target arrow_ep-patch --parallel "$NPROC"
arrow_source="$ASAN_VELOX_BUILD_DIR/CMake/resolve_dependency_modules/arrow/arrow_ep/src/arrow_ep/cpp"
[[ -f "$arrow_source/CMakeLists.txt" ]] || \
  die "Matching SDK Arrow source missing: $arrow_source"

# A cached archive must still incorporate the reviewed source patch.
cmake --build "$ASAN_VELOX_BUILD_DIR" --target velox --parallel "$NPROC"
PANGROVA_FUNCTION_INSPECTOR_ASAN=1 bash "$ASAN_SCRIPT_DIR/record-function-provenance.sh" "$VELOX_HOME" \
  "$ASAN_VELOX_BUILD_DIR/lib/libvelox.a"

[[ -f "$ASAN_VELOX_BUILD_DIR/lib/libvelox.a" ]] || \
  die "ASan Velox archive was not produced"
grep -Eq '^CMAKE_CXX_FLAGS:.*-fsanitize=address' \
  "$ASAN_VELOX_BUILD_DIR/CMakeCache.txt" || \
  die "ASan Velox cache does not contain address-sanitizer flags"

arrow_build="$WORK_DIR/arrow-flight-asan-build"
arrow_platform_args=()
if [[ "$(uname -s)" == "Darwin" ]]; then
  arrow_platform_args+=(
    "-DCMAKE_C_VISIBILITY_PRESET=default" "-DCMAKE_CXX_VISIBILITY_PRESET=default"
    "-DCMAKE_VISIBILITY_INLINES_HIDDEN=OFF"
    "-DCMAKE_SHARED_LINKER_FLAGS=$ASAN_FLAGS -Wl,-unexported_symbols_list,\"$ASAN_SCRIPT_DIR/../arrow_unexported_symbols_macos.list\"")
fi
cmake -S "$arrow_source" -B "$arrow_build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS="$ASAN_FLAGS" -DCMAKE_CXX_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_C_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_CXX_FLAGS_RELEASE="-O1 -g1 -DNDEBUG" \
    -DCMAKE_EXE_LINKER_FLAGS="$ASAN_FLAGS" -DCMAKE_SHARED_LINKER_FLAGS="$ASAN_FLAGS" \
    -DCMAKE_INSTALL_PREFIX="$ASAN_PREFIX" \
    -DCMAKE_PREFIX_PATH="$ASAN_PREFIX;$VELOX_HOME/deps-install" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POLICY_DEFAULT_CMP0177=NEW \
    -Dabsl_DIR:PATH="$ASAN_PREFIX/lib/cmake/absl" \
    -DProtobuf_DIR:PATH="$ASAN_PREFIX/lib/cmake/protobuf" \
    -DgRPC_DIR:PATH="$ASAN_PREFIX/lib/cmake/grpc" \
    -Dre2_DIR:PATH="$ASAN_PREFIX/lib/cmake/re2" \
    -DARROW_BUILD_STATIC=OFF -DARROW_BUILD_SHARED=ON -DARROW_OPTIONAL_INSTALL=ON \
    -DARROW_FLIGHT=ON -DARROW_IPC=ON -DARROW_COMPUTE=OFF -DARROW_CSV=OFF \
    -DARROW_DATASET=OFF -DARROW_FILESYSTEM=OFF -DARROW_JSON=OFF -DARROW_PARQUET=OFF \
    -DARROW_WITH_RE2=OFF -DARROW_WITH_UTF8PROC=OFF -DARROW_WITH_SNAPPY=OFF \
    -DARROW_WITH_ZLIB=OFF -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
    -Dlz4_SOURCE=BUNDLED -DARROW_WITH_BROTLI=OFF -DARROW_WITH_BZ2=OFF \
    -DARROW_DEPENDENCY_USE_SHARED=OFF -DARROW_PROTOBUF_USE_SHARED=OFF \
    -DProtobuf_SOURCE=SYSTEM -DgRPC_SOURCE=SYSTEM \
    ${asan_ssl_args[@]+"${asan_ssl_args[@]}"} ${arrow_platform_args[@]+"${arrow_platform_args[@]}"}
cmake --build "$arrow_build" --target arrow_flight_shared --parallel "$NPROC"
if [[ "$(uname -s)" == "Darwin" ]]; then
  python3 "$ASAN_SCRIPT_DIR/isolate-arrow-macos-exports.py" --build-dir "$arrow_build" \
    --seed "$ASAN_SCRIPT_DIR/../arrow_unexported_symbols_macos.list"
  cmake -S "$arrow_source" -B "$arrow_build" \
    "-DCMAKE_SHARED_LINKER_FLAGS=$ASAN_FLAGS -Wl,-unexported_symbols_list,\"$arrow_build/arrow-private-symbols.list\""
  cmake --build "$arrow_build" --target arrow_flight_shared --parallel "$NPROC"
  python3 "$ASAN_SCRIPT_DIR/isolate-arrow-macos-exports.py" --build-dir "$arrow_build" --verify
fi
cmake --install "$arrow_build"
python3 "$ASAN_SCRIPT_DIR/../patch_flight_types.py" --deps-prefix "$ASAN_PREFIX"

cat > "$PANGROVA_HOME/asan-env.sh" <<EOF
export PANGROVA_DEPS_PREFIX="$ASAN_PREFIX"
export ARROW_FLIGHT_HOME="$ASAN_PREFIX"
export Arrow_DIR="$ASAN_PREFIX/lib/cmake/Arrow"
export re2_DIR="$ASAN_PREFIX/lib/cmake/re2"
export gRPC_DIR="$ASAN_PREFIX/lib/cmake/grpc"
export absl_DIR="$ASAN_PREFIX/lib/cmake/absl"
export Protobuf_DIR="$ASAN_PREFIX/lib/cmake/protobuf"
export VELOX_BUILD_DIR="$ASAN_VELOX_BUILD_DIR"
export PANGROVA_ASAN_PROTOBUF_LIB="$ASAN_PREFIX/lib/libprotobuf.a"
export PANGROVA_ASAN_ABSL_DIR="$ASAN_PREFIX/lib/cmake/absl"
export PANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_ASAN_FOLLY_PREFIX"
EOF
if [[ "$(uname -s)" == "Darwin" ]]; then
  printf 'export OPENSSL_ROOT_DIR="%s"\n' "$OPENSSL_PREFIX" >> "$PANGROVA_HOME/asan-env.sh"
fi

printf 'ASan dependencies ready: %s\n' "$ASAN_VELOX_BUILD_DIR"
