#!/usr/bin/env bash
# ==============================================================================
# setup-native-build-env.sh — Hermetic native build environment for Pangrova
#
# Builds ALL C++ dependencies from source into a single prefix tree.
# No Homebrew/MacPorts/system protobuf/gRPC contamination.
#
# Dependency chain (order matters):
#   1. abseil-cpp  → required by protobuf and gRPC
#   2. protobuf    → required by gRPC and OpenTelemetry
#   3. RE2         → canonical headers/archive shared by Velox and gRPC
#   4. gRPC        → required by Arrow Flight and OpenTelemetry OTLP exporter
#   5. Velox       → execution engine (bundles folly/glog/gflags/fmt/boost)
#
# Usage:
#   ./pangrova-worker-native/bin/setup-native-build-env.sh
#   ./pangrova-worker-native/bin/setup-native-build-env.sh --check-tools
#   SKIP_VELOX=1 ./pangrova-worker-native/bin/setup-native-build-env.sh
#   SKIP_DEPS=1 ./pangrova-worker-native/bin/setup-native-build-env.sh
#
# Output layout:
#   ~/.pangrova/native-deps/     — abseil + protobuf + RE2 + gRPC (hermetic)
#   ~/.pangrova/velox/           — Velox source + build + deps-install
#   ~/.pangrova/env.sh           — source this to activate all paths
# ==============================================================================
set -euo pipefail
PANGROVA_BOOTSTRAP_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PANGROVA_HOME="${PANGROVA_HOME:-$HOME/.pangrova}"
DEPS_PREFIX="${PANGROVA_DEPS_PREFIX:-$PANGROVA_HOME/native-deps}"
VELOX_DIR="${VELOX_HOME:-$PANGROVA_HOME/velox}"
VELOX_REF="${VELOX_REF:-v2026.04.17.00}"
SKIP_VELOX="${SKIP_VELOX:-0}"
SKIP_DEPS="${SKIP_DEPS:-0}"
PANGROVA_PRODUCTION_BUILD="${PANGROVA_PRODUCTION_BUILD:-OFF}"

GRPC_VERSION="${GRPC_VERSION:-v1.65.0}"
RE2_VERSION="${RE2_VERSION:-2024-07-02}"
ABSEIL_VERSION="${ABSEIL_VERSION:-20240116.2}"
PROTOBUF_VERSION="${PROTOBUF_VERSION:-v25.3}"

# Fall back to /tmp rather than the current directory: TMPDIR is unset on most
# Linux CI runners, which would otherwise place the Velox/dependency build tree
# inside the repository working copy.
WORK_DIR="${PANGROVA_NATIVE_BUILD_DIR:-${TMPDIR:-/tmp}/pangrova-native-build}"
OS="$(uname -s)"
ARCH="$(uname -m)"
# A caller's explicit build limit must reach both our builds and Velox's
# dependency installer. CPU count alone does not describe available memory.
NPROC="${CMAKE_BUILD_PARALLEL_LEVEL:-${BUILD_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}}"
if [[ ! "$NPROC" =~ ^[1-9][0-9]*$ ]]; then
  printf 'ERROR: build parallelism must be a positive integer\n' >&2
  exit 1
fi
export CMAKE_BUILD_PARALLEL_LEVEL="$NPROC"
export BUILD_THREADS="$NPROC"

log()  { printf "\n=== %s ===\n" "$*"; }
step() { printf -- "--- %s\n" "$*"; }
die()  { printf "ERROR: %s\n" "$*" >&2; exit 1; }

# Cache only a complete checkout at the requested revision. A failed network
# transfer must not leave a directory that a subsequent build treats as source.
checkout_dependency() {
  local repository="$1" revision="$2" destination="$3"
  if [[ ! -d "$destination" ]]; then
    local attempt checkout
    for attempt in 1 2 3; do
      checkout="$(mktemp -d "$WORK_DIR/source-checkout.XXXXXX")"
      if git -c http.version=HTTP/1.1 clone --depth 1 --branch "$revision" \
          "$repository" "$checkout/repository"; then
        mv "$checkout/repository" "$destination"
        rmdir "$checkout"
        break
      fi
      step "Dependency transfer failed (attempt $attempt/3); incomplete checkout retained at $checkout"
      [[ "$attempt" == 3 ]] || sleep 2
    done
  fi
  local expected actual
  expected="$(git -C "$destination" rev-parse --verify "$revision^{commit}")" \
    || die "Dependency cache does not contain pinned revision $revision: $destination"
  actual="$(git -C "$destination" rev-parse --verify HEAD)"
  [[ "$actual" == "$expected" ]] \
    || die "Dependency cache revision differs from $revision: $destination"
}

check_prerequisites() {
  if [[ "$OS" == "Darwin" && -x "$PANGROVA_HOME/homebrew/bin/brew" ]]; then
    export PATH="$PANGROVA_HOME/homebrew/bin:$PANGROVA_HOME/homebrew/sbin:$PATH"
  fi
  for cmd in cmake ninja git cargo rustc; do
    command -v "$cmd" >/dev/null 2>&1 || die "$cmd not found. Install it first."
  done
  if [[ "$OS" == "Darwin" ]]; then
    xcode-select -p >/dev/null 2>&1 || die "Xcode CLT required. Run: xcode-select --install"
    command -v brew >/dev/null 2>&1 \
      || die "Homebrew is required by Velox's macOS dependency setup. Install it from https://brew.sh/."
  fi
}

verify_build_tools() {
  log "Verifying existing build tools (no package-manager mutation)"
  for tool in cmake ninja autoconf automake pkg-config; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found. Install it first."
  done
  if [[ "$OS" == "Darwin" ]]; then
    command -v glibtoolize >/dev/null 2>&1 || die "GNU libtool not found. Install it first."
  elif [[ "$OS" == "Linux" ]]; then
    command -v libtoolize >/dev/null 2>&1 || die "GNU libtool not found. Install it first."
  else
    die "Unsupported OS: $OS"
  fi
  command -v ccache >/dev/null 2>&1 || step "ccache not installed (optional)"
}

configure_compiler() {
  if [[ "$OS" != "Darwin" ]]; then
    export CC="${CC:-gcc}"
    export CXX="${CXX:-g++}"
    return
  fi
  local llvm_prefix
  if [[ -x "$PANGROVA_HOME/llvm/bin/clang++" ]]; then
    llvm_prefix="$PANGROVA_HOME/llvm"
  else
    llvm_prefix="$(brew --prefix llvm 2>/dev/null || true)"
  fi
  [[ -x "$llvm_prefix/bin/clang++" ]] || \
    die "Clang 15 or newer is required by Velox; install it under $PANGROVA_HOME/llvm"
  export PATH="$PANGROVA_HOME/bin:$llvm_prefix/bin:$PATH"
  for tool in bison flex; do
    if [[ -d "$PANGROVA_HOME/homebrew/opt/$tool/bin" ]]; then
      export PATH="$PANGROVA_HOME/homebrew/opt/$tool/bin:$PATH"
    fi
  done
  export CC="$llvm_prefix/bin/clang"
  export CXX="$llvm_prefix/bin/clang++"
  step "Using $($CXX --version | head -1)"
}

ensure_folly_compiler_abi() {
  [[ "$OS" == "Darwin" ]] || return 0
  local folly_source="$VELOX_DIR/deps-download/folly"
  local folly_archive="$VELOX_DIR/deps-install/lib/libfolly.a"
  # Consume nm fully: grep -q closes the pipe early and pipefail turns a
  # matching ABI into nm's broken-pipe failure on a large archive.
  if [[ -f "$folly_archive" ]] &&
      nm -gU "$folly_archive" 2>/dev/null | grep 'NSt3__13pmr15memory_resource' >/dev/null; then
    return
  fi
  [[ -f "$folly_source/CMakeLists.txt" ]] || \
    die "Folly source not found at $folly_source"
  log "Rebuilding Folly with Pangrova Clang/libc++ ABI"
  rm -rf "$folly_source/_build-pangrova"
  cmake -S "$folly_source" -B "$folly_source/_build-pangrova" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_CXX_STANDARD=20 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_PREFIX_PATH="$VELOX_DIR/deps-install;$PANGROVA_HOME/homebrew" \
    -DCMAKE_INSTALL_PREFIX="$VELOX_DIR/deps-install" \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_TESTS=OFF
  cmake --build "$folly_source/_build-pangrova" --target install -j "$NPROC"
  nm -gU "$folly_archive" 2>/dev/null | \
    grep 'NSt3__13pmr15memory_resource' >/dev/null || \
    die "Rebuilt Folly still has an incompatible memory_resource ABI"
  step "Folly rebuilt with Pangrova Clang/libc++ ABI"
}

build_abseil() {
  log "Building abseil-cpp $ABSEIL_VERSION"
  local src="$WORK_DIR/abseil-cpp"
  checkout_dependency https://github.com/abseil/abseil-cpp.git "$ABSEIL_VERSION" "$src"
  local arch_flags=()
  if [[ "$OS" == "Darwin" && "$ARCH" == "arm64" ]]; then
    arch_flags+=(-DCMAKE_OSX_ARCHITECTURES=arm64)
    # abseil 20240116.2 unconditionally iterates x86_64+arm64 for HWAES flags
    # on Apple (line 41: foreach(_arch IN ITEMS "x86_64" "arm64")).
    # Patch to only use arm64 when building single-arch.
    sed -i.bak 's/foreach(_arch IN ITEMS "x86_64" "arm64")/foreach(_arch IN ITEMS "arm64")/' \
      "$src/absl/copts/AbseilConfigureCopts.cmake"
  fi
  cmake -S "$src" -B "$src/build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" \
    -DCMAKE_PREFIX_PATH="$DEPS_PREFIX" \
    -DCMAKE_IGNORE_PATH="/opt/local;/opt/homebrew" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_BUILD_TESTING=OFF \
    "${arch_flags[@]}"
  cmake --build "$src/build" -j "$NPROC"
  cmake --install "$src/build"
  step "abseil installed to $DEPS_PREFIX"
}

build_protobuf() {
  log "Building protobuf $PROTOBUF_VERSION"
  local src="$WORK_DIR/protobuf"
  checkout_dependency https://github.com/protocolbuffers/protobuf.git "$PROTOBUF_VERSION" "$src"
  cmake -S "$src" -B "$src/build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" \
    -DCMAKE_PREFIX_PATH="$DEPS_PREFIX" \
    -DCMAKE_IGNORE_PATH="/opt/local;/opt/homebrew" \
    -DCMAKE_FIND_USE_CMAKE_SYSTEM_PATH=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -Dabsl_DIR="$DEPS_PREFIX/lib/cmake/absl" \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_EXAMPLES=OFF \
    -Dprotobuf_ABSL_PROVIDER=package \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF
  cmake --build "$src/build" -j "$NPROC"
  cmake --install "$src/build"
  step "protobuf installed to $DEPS_PREFIX"
}

build_re2() {
  log "Building the canonical RE2 $RE2_VERSION package"
  local src="$WORK_DIR/re2"
  checkout_dependency https://github.com/google/re2.git "$RE2_VERSION" "$src"
  cmake -S "$src" -B "$src/_cmake_build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_PREFIX_PATH="$DEPS_PREFIX" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -Dabsl_DIR="$DEPS_PREFIX/lib/cmake/absl" \
    -DBUILD_SHARED_LIBS=OFF -DRE2_BUILD_TESTING=OFF
  cmake --build "$src/_cmake_build" --parallel "$NPROC"
  cmake --install "$src/_cmake_build"
  step "Canonical RE2 installed to $DEPS_PREFIX"
}

build_grpc() {
  log "Building gRPC $GRPC_VERSION"
  local src="$WORK_DIR/grpc"
  local ssl_args=()
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
    ssl_args+=(-DBUILD_SHARED_LIBS=OFF -DOPENSSL_USE_STATIC_LIBS=TRUE)
    ssl_args+=(-DCMAKE_FIND_LIBRARY_SUFFIXES=.a)
  fi
  if [[ "$OS" == "Darwin" ]]; then
    ssl_args+=("-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)")
  fi
  checkout_dependency https://github.com/grpc/grpc.git "$GRPC_VERSION" "$src"
  # Package providers supply abseil, protobuf, and zlib. Only the providers
  # configured as modules need their gRPC submodules initialized.
  # SSL is a package provider (the same OpenSSL used by the other Native
  # libraries). Fetching BoringSSL is unnecessary and adds an unrelated build
  # dependency. Force HTTP/1.1 for Git transports that fail HTTP/2 framing.
  git -c http.version=HTTP/1.1 -C "$src" submodule update --init --depth 1 \
    third_party/cares/cares
  cmake -S "$src" -B "$src/_cmake_build" -GNinja \
    -U CMAKE_CXX_FLAGS \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" \
    -DCMAKE_PREFIX_PATH="$DEPS_PREFIX" \
    -DCMAKE_IGNORE_PATH="/opt/local;/opt/homebrew" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -Dabsl_DIR="$DEPS_PREFIX/lib/cmake/absl" \
    -DProtobuf_DIR="$DEPS_PREFIX/lib/cmake/protobuf" \
    -Dre2_DIR:PATH="$DEPS_PREFIX/lib/cmake/re2" \
    -DgRPC_INSTALL=ON \
    -DgRPC_BUILD_TESTS=OFF \
    -DgRPC_BUILD_CSHARP_EXT=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=module \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package \
    "${ssl_args[@]}"
  cmake --build "$src/_cmake_build" -j "$NPROC"
  cmake --install "$src/_cmake_build"
  step "gRPC installed to $DEPS_PREFIX"
}

build_velox() {
  log "Building Velox $VELOX_REF"
  local GRPC_VERSION="$GRPC_VERSION"
  local PROTOBUF_VERSION="$PROTOBUF_VERSION"
  local ABSEIL_VERSION="$ABSEIL_VERSION"
  if [[ -d "$VELOX_DIR" ]]; then
    step "Updating existing Velox source"
    cd "$VELOX_DIR"
    if ! git checkout "$VELOX_REF"; then
      local attempt
      for attempt in {1..5}; do
        git -c http.version=HTTP/1.1 fetch --depth 1 origin "$VELOX_REF" && break
        [[ "$attempt" == "5" ]] && die "Failed to fetch Velox ref $VELOX_REF"
      done
      git checkout "$VELOX_REF"
    fi
  else
    step "Cloning Velox"
    git clone --depth 1 --branch "$VELOX_REF" \
      https://github.com/facebookincubator/velox.git "$VELOX_DIR"
  fi
  cd "$VELOX_DIR"
  bash "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/apply-velox-patches.sh" apply "$VELOX_DIR"
  if [[ "$OS" == "Darwin" ]]; then
    export PROMPT_ALWAYS_RESPOND="${PROMPT_ALWAYS_RESPOND:-Y}"
    export CURL_OPTIONS="${CURL_OPTIONS:---retry 20 --retry-all-errors --connect-timeout 30}"
    source ./scripts/setup-macos.sh

    # Upstream stores archives inside source directories that it deletes on retry.
    wget_and_untar() {
      local url="$1"
      local dir="$2"
      if [[ "$url" =~ ^https://github.com/([^/]+/[^/]+)/archive/refs/tags/(.+)\.tar\.gz$ ]]; then
        url="https://codeload.github.com/${BASH_REMATCH[1]}/tar.gz/refs/tags/${BASH_REMATCH[2]}"
      fi
      local archive_cache="${VELOX_ARCHIVE_CACHE:-$HOME/.pangrova/velox/archive-cache}"
      local archive="$archive_cache/$dir-$(basename "$url")"
      mkdir -p "$archive_cache" "$DEPENDENCY_DIR"
      if [[ ! -f "$archive" ]] || ! gzip -t "$archive" 2>/dev/null; then
        curl ${CURL_OPTIONS:+${CURL_OPTIONS}} --continue-at - -L "$url" -o "$archive"
      fi
      rm -rf "$DEPENDENCY_DIR/$dir"
      mkdir -p "$DEPENDENCY_DIR/$dir"
      tar -xz --strip-components=1 --no-same-owner \
        -f "$archive" -C "$DEPENDENCY_DIR/$dir"
    }

    install_fbthrift() {
      wget_and_untar \
        "https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        fbthrift
      local type_system_builder="$DEPENDENCY_DIR/fbthrift/thrift/lib/cpp2/dynamic/TypeSystemBuilder.cpp"
      grep -Fq 'std::invoke([&]() -> TSDefinition' "$type_system_builder" || \
        die "Unexpected fbthrift TypeSystemBuilder.cpp at $FB_OS_VERSION"
      sed -i.bak \
        -e '/auto uninitDef = std::invoke/i\
    const auto uriCopy = uri;' \
        -e 's/StructNode{uri/StructNode{uriCopy/' \
        -e 's/UnionNode{uri/UnionNode{uriCopy/' \
        -e 's/EnumNode{uri/EnumNode{uriCopy/' \
        -e 's/OpaqueAliasNode{uri/OpaqueAliasNode{uriCopy/' \
        "$type_system_builder"
      rm -f "$type_system_builder.bak"
      cmake_install_dir fbthrift \
        -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
      touch "$INSTALL_PREFIX/.pangrova-fbthrift-ready"
    }

    install_aws_deps() {
      local aws_source="$DEPENDENCY_DIR/aws-sdk-cpp"
      if [[ ! -d "$aws_source/.git" ]]; then
        rm -rf "$aws_source"
        local attempt
        for attempt in {1..5}; do
          git -c http.version=HTTP/1.1 clone --depth 1 --branch "$AWS_SDK_VERSION" \
            --recurse-submodules --shallow-submodules \
            https://github.com/aws/aws-sdk-cpp.git "$aws_source" && break
          rm -rf "$aws_source"
          [[ "$attempt" == "5" ]] && die "Failed to clone AWS SDK $AWS_SDK_VERSION"
        done
      fi
      cmake_install_dir aws-sdk-cpp \
        -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
        -DBUILD_SHARED_LIBS=OFF \
        -DMINIMIZE_SIZE=ON \
        -DENABLE_TESTING=OFF \
        '-DBUILD_ONLY=s3;identity-management'
    }

    MACOS_VELOX_DEPS=""
    BUILD_FAISS=false
    if [[ ! -f "$INSTALL_PREFIX/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]]; then
      run_and_time install_aws_deps
    fi
    if [[ -f "$INSTALL_PREFIX/.pangrova-fbthrift-ready" ]]; then
      run_and_time install_xsimd
      run_and_time install_stemmer
      run_and_time install_arrow
      run_and_time install_duckdb_clang
      run_and_time install_geos
      run_and_time install_s2geometry
      run_and_time install_faiss
    else
      install_velox_deps
    fi
  elif [[ "$OS" == "Linux" ]]; then
    local archive_patch="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-codeload-archives.patch"
    if ! git apply --reverse --check "$archive_patch" >/dev/null 2>&1; then
      git apply --check "$archive_patch"
      git apply "$archive_patch"
    fi
    ./scripts/setup-ubuntu.sh
    # setup-ubuntu.sh installs core Velox dependencies but leaves cloud adapters
    # to an explicit command. The S3-enabled Velox configure requires AWSSDK.
    if [[ ! -f "${INSTALL_PREFIX:-/usr/local}/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]]; then
      ./scripts/setup-ubuntu.sh install_aws_deps
    fi
  fi

  ensure_folly_compiler_abi

  # ===========================================================================
  # Velox CMake configuration
  #
  # Key principles:
  #   1. VELOX_MONO_LIBRARY=ON: single libvelox.a (required for force_load)
  #   2. CMAKE_PREFIX_PATH includes DEPS_PREFIX so Velox uses our hermetic
  #      protobuf v4.25.3 + abseil (ABI-unified with gRPC/OTel)
  #   3. CMAKE_PREFIX_PATH also includes deps-install for AWS SDK
  #   4. CMAKE_POLICY_VERSION_MINIMUM=3.5 for xsimd compatibility
  #   5. Disable modules we don't need (GEO, TPCH, TPCDS)
  #   6. Enable modules we need (ARROW, PARQUET, S3, HIVE_CONNECTOR)
  #
  # CRITICAL: Velox MUST use the same protobuf as gRPC/OTel (hermetic v4.25.3).
  # Without CMAKE_PREFIX_PATH pointing to DEPS_PREFIX, Velox FetchContent's
  # protobuf v3.21.8 which has incompatible ABI (std::once_flag vs absl::once_flag).
  # ===========================================================================
  local velox_cache="_build/release/CMakeCache.txt"
  if [[ -f "$velox_cache" ]]; then
    local cached_cxx
    cached_cxx=$(sed -n 's|^CMAKE_CXX_COMPILER:\(FILEPATH\|STRING\)=||p' "$velox_cache")
    if [[ -n "$cached_cxx" && "$cached_cxx" != "$CXX" ]]; then
      step "Velox compiler changed ($cached_cxx -> $CXX); clearing generated Release build"
      rm -rf _build/release
    fi
  fi
  local velox_ssl_args=()
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
    velox_ssl_args+=(-DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
    velox_ssl_args+=(-DCMAKE_FIND_LIBRARY_SUFFIXES=.a -DOPENSSL_USE_STATIC_LIBS=TRUE)
  fi
  local velox_prefix_path="$DEPS_PREFIX;$VELOX_DIR/deps-install"
  if [[ "$OS" == "Darwin" ]]; then
    velox_ssl_args+=("-DOPENSSL_ROOT_DIR=$PANGROVA_HOME/homebrew/opt/openssl@3")
    velox_ssl_args+=("-DFLEX_INCLUDE_DIR=$PANGROVA_HOME/homebrew/opt/flex/include")
    velox_prefix_path+=";$PANGROVA_HOME/homebrew"
  fi
  cmake -B _build/release -GNinja \
    -DPANGROVA_FUNCTION_SELECTION_TOOLS="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../../pangrova-protocol/tools/function-contracts" \
    -DPANGROVA_FUNCTION_SELECTION_DECLARATION="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../../pangrova-protocol/src/main/resources/function-contracts/execution-selection.json" \
    -DPANGROVA_FUNCTION_SDK_PATCH_FILE="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-function-metadata-snapshot.patch" \
    -DPython3_EXECUTABLE="${PANGROVA_FUNCTION_CONTRACT_PYTHON:-python3}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="$velox_prefix_path" \
    "${velox_ssl_args[@]}" \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POLICY_DEFAULT_CMP0167=NEW \
    -Dsimdjson_SOURCE=BUNDLED \
    -DVELOX_MONO_LIBRARY=ON \
    -Dabsl_DIR:PATH="$DEPS_PREFIX/lib/cmake/absl" \
    -Dre2_DIR:PATH="$DEPS_PREFIX/lib/cmake/re2" \
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
  cmake --build _build/release -j "$NPROC"
  bash "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/record-function-provenance.sh" "$VELOX_DIR" \
    "$VELOX_DIR/_build/release/lib/libvelox.a"
  step "Velox built at $VELOX_DIR/_build/release"
}

verify_re2() {
  log "Verifying the complete canonical RE2 package"
  cmake -S "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../cmake/dependency-checks/re2" \
    -B "$WORK_DIR/re2-contract-build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release -DPANGROVA_DEPS_PREFIX="$DEPS_PREFIX"
  cmake --build "$WORK_DIR/re2-contract-build" --parallel "$NPROC"
  "$WORK_DIR/re2-contract-build/pangrova_re2_contract"
}

build_arrow_flight() {
  log "Building Arrow Flight from Velox's Arrow source"
  # Velox builds Arrow via ExternalProject but does NOT enable Flight.
  # We build IPC (including its declared codecs) and Flight from the same source
  # to ensure ABI compatibility (same Arrow version, same compiler flags).
  local arrow_ep="$VELOX_DIR/_build/release/CMake/resolve_dependency_modules/arrow/arrow_ep"
  local arrow_src="$arrow_ep/src/arrow_ep"

  if [[ ! -d "$arrow_src/cpp" ]]; then
    die "Arrow source not found at $arrow_src/cpp — build Velox first"
  fi

  step "Building libarrow_flight from $arrow_src"
  local arrow_static=OFF arrow_shared=ON arrow_target=arrow_flight_shared arrow_parquet=OFF
  local arrow_build="$WORK_DIR/arrow-flight-build"
  local arrow_platform_args=()
  if [[ "$OS" == "Darwin" ]]; then
    arrow_platform_args+=("-DOPENSSL_ROOT_DIR=$PANGROVA_HOME/homebrew/opt/openssl@3")
    arrow_platform_args+=("-DCMAKE_C_VISIBILITY_PRESET=default" "-DCMAKE_CXX_VISIBILITY_PRESET=default"
      "-DCMAKE_VISIBILITY_INLINES_HIDDEN=OFF")
    arrow_platform_args+=("-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-unexported_symbols_list,\"$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../arrow_unexported_symbols_macos.list\"")
  fi
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
    arrow_static=ON
    arrow_shared=OFF
    arrow_target=arrow_flight_static
    arrow_parquet=ON
    arrow_build="$WORK_DIR/arrow-flight-production-build"
  fi
  cmake -S "$arrow_src/cpp" -B "$arrow_build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" \
    -DCMAKE_PREFIX_PATH="$DEPS_PREFIX;$VELOX_DIR/deps-install" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POLICY_DEFAULT_CMP0177=NEW \
    "${arrow_platform_args[@]}" \
    -DARROW_BUILD_STATIC="$arrow_static" \
    -DARROW_BUILD_SHARED="$arrow_shared" \
    -DARROW_OPTIONAL_INSTALL=ON \
    -DARROW_FLIGHT=ON \
    -DARROW_IPC=ON \
    -DARROW_COMPUTE=OFF \
    -DARROW_CSV=OFF \
    -DARROW_DATASET=OFF \
    -DARROW_FILESYSTEM=OFF \
    -DARROW_JSON=OFF \
    -DARROW_PARQUET="$arrow_parquet" \
    -DARROW_WITH_RE2=OFF \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_WITH_SNAPPY=OFF \
    -DARROW_WITH_ZLIB=OFF \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -Dlz4_SOURCE=BUNDLED \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_WITH_BZ2=OFF \
    -DARROW_DEPENDENCY_USE_SHARED=OFF \
    -DARROW_PROTOBUF_USE_SHARED=OFF \
    -DProtobuf_SOURCE=SYSTEM \
    -DgRPC_SOURCE=SYSTEM
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
    cmake --build "$arrow_build" -j "$NPROC"
  else
    cmake --build "$arrow_build" --target "$arrow_target" -j "$NPROC"
  fi
  if [[ "$OS" == "Darwin" && "$arrow_shared" == "ON" ]]; then
    # Preserve precisely the visible Arrow ABI from the first link. Static RPC,
    # TLS and compression dependencies must not become process-global exports.
    python3 "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/isolate-arrow-macos-exports.py" --build-dir "$arrow_build" \
      --seed "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../arrow_unexported_symbols_macos.list"
    cmake -S "$arrow_src/cpp" -B "$arrow_build" \
      "-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-unexported_symbols_list,\"$arrow_build/arrow-private-symbols.list\""
    cmake --build "$arrow_build" --target "$arrow_target" -j "$NPROC"
    python3 "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/isolate-arrow-macos-exports.py" --build-dir "$arrow_build" --verify
  fi
  cmake --install "$arrow_build"
  python3 "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patch_flight_types.py" --deps-prefix "$DEPS_PREFIX"
  step "Arrow Flight package installed to $DEPS_PREFIX"
}

write_env() {
  log "Writing environment file"
  mkdir -p "$PANGROVA_HOME"

  # Backup existing env.sh before overwriting
  if [[ -f "$PANGROVA_HOME/env.sh" ]]; then
    cp "$PANGROVA_HOME/env.sh" "$PANGROVA_HOME/env.sh.bak"
    step "Backed up existing env.sh to env.sh.bak"
  fi

  cat > "$PANGROVA_HOME/env.sh" << 'ENVEOF'
# Pangrova native build environment — source this file before building.
# Generated by setup-native-build-env.sh on GENERATED_TIMESTAMP
# Script version: 1.0.0

export PANGROVA_HOME="PANGROVA_HOME_VAL"
export PANGROVA_DEPS_PREFIX="DEPS_PREFIX_VAL"
export VELOX_HOME="VELOX_DIR_VAL"
export VELOX_BUILD_DIR="VELOX_DIR_VAL/_build/release"
export VELOX_INSTALL_PREFIX="VELOX_DIR_VAL"
LLVM_ENV

# System dependency isolation — blocks /opt/local (MacPorts) and /opt/homebrew
export CMAKE_IGNORE_PATH="/opt/local;/opt/homebrew"

# Idempotent PATH update (safe to source multiple times)
case ":$PATH:" in
  *":DEPS_PREFIX_VAL/bin:"*) ;;
  *) export PATH="DEPS_PREFIX_VAL/bin:$PATH" ;;
esac
ENVEOF

  # Substitute actual values (heredoc uses 'ENVEOF' to prevent expansion)
  sed -i.tmp \
    -e "s|PANGROVA_HOME_VAL|$PANGROVA_HOME|g" \
    -e "s|DEPS_PREFIX_VAL|$DEPS_PREFIX|g" \
    -e "s|VELOX_DIR_VAL|$VELOX_DIR|g" \
    -e "s|GENERATED_TIMESTAMP|$(date -u +%Y-%m-%dT%H:%M:%SZ)|g" \
    "$PANGROVA_HOME/env.sh"
  if [[ "$OS" == "Darwin" ]]; then
    local llvm_env
    llvm_env="export CC=\"$CC\"\nexport CXX=\"$CXX\"\nexport PATH=\"$(dirname "$CXX"):\$PATH\""
    sed -i.tmp -e "s|LLVM_ENV|$llvm_env|" "$PANGROVA_HOME/env.sh"
  else
    sed -i.tmp -e '/LLVM_ENV/d' "$PANGROVA_HOME/env.sh"
  fi
  rm -f "$PANGROVA_HOME/env.sh.tmp"

  step "Activate with: source $PANGROVA_HOME/env.sh"
}

verify() {
  log "Verifying installation"
  local ok=true
  for f in bin/protoc bin/grpc_cpp_plugin lib/cmake/protobuf/protobuf-config.cmake lib/cmake/grpc/gRPCConfig.cmake; do
    if [[ ! -e "$DEPS_PREFIX/$f" ]]; then
      printf "  MISSING: %s/%s\n" "$DEPS_PREFIX" "$f"
      ok=false
    fi
  done
  if [[ "$SKIP_VELOX" != "1" ]] && [[ ! -f "$VELOX_DIR/_build/release/lib/libvelox.a" ]] && \
     [[ ! -f "$VELOX_DIR/_build/release/libvelox.a" ]]; then
    step "WARNING: libvelox.a not found (may be in a different path)"
  fi
  if $ok; then step "All checks passed"; else die "Verification failed"; fi

  # Structured version output
  step "Installed components:"
  printf "  %-14s %s → %s\n" "protoc:" \
    "$("$DEPS_PREFIX/bin/protoc" --version 2>/dev/null || echo 'N/A')" \
    "$DEPS_PREFIX/bin/protoc"
  printf "  %-14s %s → %s\n" "libprotobuf:" "$PROTOBUF_VERSION" \
    "$DEPS_PREFIX/lib/cmake/protobuf/"
  printf "  %-14s %s → %s\n" "gRPC:" "$GRPC_VERSION" \
    "$DEPS_PREFIX/lib/cmake/grpc/"
  printf "  %-14s %s → %s\n" "abseil:" "$ABSEIL_VERSION" \
    "$DEPS_PREFIX/lib/cmake/absl/"
}

main() {
  [[ "$#" == 0 || ( "$#" == 1 && "$1" == "--check-tools" ) ]] \
    || die "Usage: setup-native-build-env.sh [--check-tools]"
  case "$PANGROVA_PRODUCTION_BUILD" in
    ON) [[ "$OS" == "Linux" ]] || die "Production Native setup currently requires Linux" ;;
    OFF) ;;
    *) die "PANGROVA_PRODUCTION_BUILD must be ON or OFF" ;;
  esac
  log "Pangrova Native Build Environment Setup"
  printf "  Platform: %s %s | Deps: %s | Velox: %s | Jobs: %s\n" \
    "$OS" "$ARCH" "$DEPS_PREFIX" "$VELOX_DIR" "$NPROC"

  check_prerequisites
  verify_build_tools
  configure_compiler
  [[ "$#" == 0 ]] || return 0
  mkdir -p "$WORK_DIR" "$DEPS_PREFIX"

  if [[ "$SKIP_DEPS" != "1" ]]; then
    build_abseil
    build_protobuf
    build_re2
    build_grpc
  else
    step "Skipping deps build (SKIP_DEPS=1)"
  fi

  verify_re2

  if [[ "$SKIP_VELOX" != "1" ]]; then
    build_velox
    build_arrow_flight
  else
    step "Skipping Velox build (SKIP_VELOX=1)"
  fi

  write_env
  verify
  log "Setup complete — source $PANGROVA_HOME/env.sh to activate"
}

main "$@"
