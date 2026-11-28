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
#   PANGROVA_NATIVE_BUILD_PHASE=dependencies|velox|arrow-flight to split build phases
#   SKIP_DEPS=1 PANGROVA_NATIVE_BUILD_PHASE=prepared-velox for verified macOS CI stages
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
PANGROVA_NATIVE_BUILD_PHASE="${PANGROVA_NATIVE_BUILD_PHASE:-all}"
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
# The macOS dependency builds need Homebrew prefixes. A developer machine may
# keep a private tree under PANGROVA_HOME; hosted runners use the system one.
if [[ "$OS" == "Darwin" ]]; then
  if [[ -x "${PANGROVA_BREW:-$PANGROVA_HOME/homebrew/bin/brew}" ]]; then
    PANGROVA_BREW="${PANGROVA_BREW:-$PANGROVA_HOME/homebrew/bin/brew}"
  else
    PANGROVA_BREW="$(command -v brew || true)"
  fi
  HOMEBREW_PREFIX="$("$PANGROVA_BREW" --prefix 2>/dev/null || true)"
  source "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/resolve-darwin-openssl.sh"
  configure_darwin_openssl "$PANGROVA_BREW" install
  BISON_PREFIX="$("$PANGROVA_BREW" --prefix bison 2>/dev/null || true)"
  FLEX_PREFIX="$("$PANGROVA_BREW" --prefix flex 2>/dev/null || true)"
fi
# A caller's explicit build limit must reach both our builds and Velox's
# dependency installer. CPU count alone does not describe available memory.
if [[ -n "${CMAKE_BUILD_PARALLEL_LEVEL:-}" || -n "${BUILD_THREADS:-}" ]]; then
  NPROC="${CMAKE_BUILD_PARALLEL_LEVEL:-$BUILD_THREADS}"
else
  NPROC="$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
  # Velox translation units are memory-hungry; an unbounded core count lets the
  # compiler be OOM-killed on 7-16 GB runners. Budget roughly 6 GB per job.
  if [[ "$OS" == "Darwin" ]]; then
    memory_gb=$(( $(sysctl -n hw.memsize) / 1073741824 ))
  else
    memory_gb=$(awk '/^MemTotal:/ {print int($2 / 1048576)}' /proc/meminfo 2>/dev/null || echo 0)
  fi
  if [[ "$memory_gb" -gt 0 ]]; then
    memory_jobs=$(( memory_gb / 6 ))
    [[ "$memory_jobs" -lt 1 ]] && memory_jobs=1
    [[ "$NPROC" -gt "$memory_jobs" ]] && NPROC="$memory_jobs"
  fi
fi
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
  for cmd in cmake ninja git cargo rustc rg; do
    command -v "$cmd" >/dev/null 2>&1 || die "$cmd not found. Install it first."
  done
  # A package-manager-installed cmake can be a shim without its Modules
  # directory. Validate and remember the working binary; the setup scripts may
  # later put such a shim earlier on PATH.
  PANGROVA_CMAKE_BIN="$(command -v cmake)"
  "$PANGROVA_CMAKE_BIN" --version >/dev/null 2>&1 \
    || die "cmake at $PANGROVA_CMAKE_BIN cannot locate its modules"
  if [[ "$OS" == "Darwin" ]]; then
    xcode-select -p >/dev/null 2>&1 || die "Xcode CLT required. Run: xcode-select --install"
    command -v brew >/dev/null 2>&1 \
      || die "Homebrew is required by Velox's macOS dependency setup. Install it from https://brew.sh/."
  fi
  if [[ "$OS" == "Linux" ]]; then
    # gRPC builds before Velox's setup-ubuntu.sh runs and needs system OpenSSL
    # and zlib headers. Fail with the package name instead of after a long
    # dependency build.
    [[ -f /usr/include/openssl/ssl.h ]] \
      || die "OpenSSL headers are required (for example: sudo apt-get install -y libssl-dev)"
    [[ -f /usr/include/zlib.h ]] \
      || die "zlib headers are required (for example: sudo apt-get install -y zlib1g-dev)"
  fi
}

verify_build_tools() {
  log "Verifying existing build tools (no package-manager mutation)"
  for tool in cmake ninja autoconf automake pkg-config patch; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found. Install it first."
  done
  if [[ "$OS" == "Darwin" ]]; then
    command -v glibtoolize >/dev/null 2>&1 || die "GNU libtool not found. Install it first."
  elif [[ "$OS" == "Linux" ]]; then
    command -v libtoolize >/dev/null 2>&1 || die "GNU libtool not found. Install it first."
  else
    die "Unsupported OS: $OS"
  fi
  if command -v ccache >/dev/null 2>&1; then
    export CCACHE_DIR="${CCACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/ccache}"
    export CCACHE_COMPRESS=1
    export CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-3G}"
    mkdir -p "$CCACHE_DIR"
    CMAKE_LAUNCHER_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    step "ccache enabled at $CCACHE_DIR (max $CCACHE_MAXSIZE)"
  else
    CMAKE_LAUNCHER_FLAGS=""
    step "ccache not installed (optional)"
  fi
}

configure_compiler() {
  if [[ "$OS" != "Darwin" ]]; then
    export CC="${CC:-gcc}"
    export CXX="${CXX:-g++}"
    return
  fi
  local llvm_prefix
  if [[ -n "${PANGROVA_LLVM_PREFIX:-}" ]]; then
    llvm_prefix="$PANGROVA_LLVM_PREFIX"
  elif [[ -x "$PANGROVA_HOME/llvm/bin/clang++" ]]; then
    llvm_prefix="$PANGROVA_HOME/llvm"
  else
    llvm_prefix="$(brew --prefix llvm 2>/dev/null || true)"
  fi
  [[ -x "$llvm_prefix/bin/clang++" ]] || \
    die "Clang 15 or newer is required by Velox; check PANGROVA_LLVM_PREFIX or install it under $PANGROVA_HOME/llvm"
  if [[ -z "${SDKROOT:-}" ]]; then
    SDKROOT="$(xcrun --sdk macosx --show-sdk-path)" || die "Unable to locate the macOS SDK"
  elif [[ "$SDKROOT" != /* ]]; then
    SDKROOT="$(xcrun --sdk "$SDKROOT" --show-sdk-path)" || die "Unable to locate the requested macOS SDK"
  fi
  [[ "$SDKROOT" == /* && "$SDKROOT" != "/" && -d "$SDKROOT/usr/include" ]] \
    || die "macOS SDK headers not found at $SDKROOT"
  export SDKROOT
  # Keep the C++ deployment baseline aligned with the packaged Lance C FFI.
  export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
  export PATH="$PANGROVA_HOME/bin:$llvm_prefix/bin:$PATH"
  [[ -n "$BISON_PREFIX" && -d "$BISON_PREFIX/bin" ]] && export PATH="$BISON_PREFIX/bin:$PATH"
  [[ -n "$FLEX_PREFIX" && -d "$FLEX_PREFIX/bin" ]] && export PATH="$FLEX_PREFIX/bin:$PATH"
  export CC="$llvm_prefix/bin/clang"
  export CXX="$llvm_prefix/bin/clang++"
  step "Using $("$CXX" --version | head -1)"
  step "Using macOS SDK $SDKROOT"
  step "Using macOS deployment target $MACOSX_DEPLOYMENT_TARGET"
  configure_darwin_cxx_runtime
}

configure_darwin_cxx_runtime() {
  local version
  version=$("$CXX" -dumpversion)
  [[ "$version" =~ ^[0-9]+(\.[0-9]+)*$ ]] || die "Cannot identify the selected Clang version: $version"
  [[ "${version%%.*}" -ge 18 ]] || return 0

  # Standalone libc++ 18+ headers can emit exception symbols unavailable in
  # the macOS system runtime. Pair the runtime with its own SDK headers.
  local headers="$SDKROOT/usr/include/c++/v1"
  [[ -f "$headers/__config" ]] || die "The macOS SDK does not contain libc++ headers: $headers"
  local checksum identity driver temporary
  checksum=$(LC_ALL=C shasum -a 256 < "$CXX")
  checksum="${checksum%% *}"
  identity=$(printf 'sdk-libcxx-v1\n%s\n%s\n%s\n' "$CXX" "$checksum" "$headers" | LC_ALL=C shasum -a 256)
  identity="${identity%% *}"
  driver="$PANGROVA_HOME/toolchain/$identity/clang++"
  mkdir -p "$(dirname "$driver")"
  temporary=$(mktemp "$driver.XXXXXXXX")
  {
    # Keep ccache's compiler content identity sensitive to the actual Clang.
    printf '#!/usr/bin/env bash\n# Clang SHA-256: %s\n' "$checksum"
    printf 'exec %q -nostdinc++ -isystem %q "$@"\n' "$CXX" "$headers"
  } > "$temporary"
  chmod +x "$temporary"
  mv -f "$temporary" "$driver"
  export CXX="$driver"
  step "Using macOS SDK libc++ headers at $headers"
  verify_darwin_exception_abi
}

verify_darwin_exception_abi() (
  local directory
  directory=$(mktemp -d "${TMPDIR:-/tmp}/pangrova-libcxx-check.XXXXXXXX")
  trap 'rm -r "$directory"' EXIT
  "$CXX" -isysroot "$SDKROOT" -std=c++20 -x c++ - -o "$directory/exception-check" <<'CPP'
#include <exception>
#include <stdexcept>
int main() {
  try {
    std::rethrow_exception(std::make_exception_ptr(std::runtime_error("expected")));
  } catch (const std::runtime_error&) {
    return 0;
  }
}
CPP
  "$directory/exception-check"
)

ensure_folly_compiler_abi() {
  [[ "$OS" == "Darwin" ]] || return 0
  local libevent_prefix="$1"
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
    -DCMAKE_PREFIX_PATH="$VELOX_DIR/deps-install;$libevent_prefix${HOMEBREW_PREFIX:+;$HOMEBREW_PREFIX}" \
    -DCMAKE_INSTALL_PREFIX="$VELOX_DIR/deps-install" \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_TESTS=OFF \
    -DCMAKE_CXX_FLAGS="-D_LIBCPP_AVAILABILITY_HAS_INIT_PRIMARY_EXCEPTION=0"
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
  # macOS Bash 3.2 treats an empty array as unset under nounset; guard its expansion.
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
    ${arch_flags[@]+"${arch_flags[@]}"}
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
    ssl_args+=("-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX")
  fi
  checkout_dependency https://github.com/grpc/grpc.git "$GRPC_VERSION" "$src"
  # Package providers supply abseil, protobuf, and zlib. Only the providers
  # configured as modules need their gRPC submodules initialized.
  # SSL is a package provider (the same OpenSSL used by the other Native
  # libraries). Fetching BoringSSL is unnecessary and adds an unrelated build
  # dependency. Force HTTP/1.1 for Git transports that fail HTTP/2 framing.
  git -c http.version=HTTP/1.1 -C "$src" submodule update --init --depth 1 \
    third_party/cares/cares
  # gRPC ships the generated sources for the installed libraries. Its
  # optional external proto downloads would add network access to configure.
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
    -DgRPC_DOWNLOAD_ARCHIVES=OFF \
    -DgRPC_ABSL_PROVIDER=package \
    -DgRPC_CARES_PROVIDER=module \
    -DgRPC_PROTOBUF_PROVIDER=package \
    -DgRPC_RE2_PROVIDER=package \
    -DgRPC_SSL_PROVIDER=package \
    -DgRPC_ZLIB_PROVIDER=package \
    ${ssl_args[@]+"${ssl_args[@]}"}
  cmake --build "$src/_cmake_build" -j "$NPROC"
  cmake --install "$src/_cmake_build"
  step "gRPC installed to $DEPS_PREFIX"
}

ensure_darwin_snappy() {
  local prefix="$1"
  local config="$prefix/lib/cmake/Snappy/SnappyConfig.cmake"
  local archive="$prefix/lib/libsnappy.a"
  if [[ -f "$config" && -f "$archive" && -f "$prefix/include/snappy.h" ]]; then
    return
  fi

  run_and_time install_snappy
  [[ -f "$config" && -f "$archive" && -f "$prefix/include/snappy.h" ]] \
    || die "Pinned Snappy package is incomplete at $prefix"
}

darwin_folly_has_release_archive() {
  local prefix="$1"
  local digest="$2"
  local stamp="$prefix/.pangrova-folly-alignment"
  [[ -f "$prefix/lib/libfolly.a" && -f "$stamp" && "$(< "$stamp")" == "Release $digest" ]]
}

prepare_velox_dependencies() {
  log "Preparing Velox $VELOX_REF dependencies"
  local GRPC_VERSION="$GRPC_VERSION"
  local PROTOBUF_VERSION="$PROTOBUF_VERSION"
  local ABSEIL_VERSION="$ABSEIL_VERSION"
  local libevent_prefix=""
  if [[ -d "$VELOX_DIR/.git" ]]; then
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
  elif [[ -d "$VELOX_DIR" ]]; then
    # The dependency cache restores deps-install without the source checkout.
    # Clone beside it and move the source in, preserving the restored prefixes.
    step "Populating Velox source into the restored dependency prefix"
    local checkout
    checkout="$(mktemp -d "$WORK_DIR/velox-checkout.XXXXXX")"
    git clone --depth 1 --branch "$VELOX_REF" \
      https://github.com/facebookincubator/velox.git "$checkout/repository"
    shopt -s dotglob
    mv "$checkout/repository"/* "$VELOX_DIR"/
    shopt -u dotglob
    rm -rf "$checkout"
  else
    step "Cloning Velox"
    git clone --depth 1 --branch "$VELOX_REF" \
      https://github.com/facebookincubator/velox.git "$VELOX_DIR"
  fi
  cd "$VELOX_DIR"
  bash "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/apply-velox-patches.sh" apply "$VELOX_DIR"

  # Velox's own dependency build downloads and compiles Folly. Apply the
  # reviewed Folly source fix and skip its examples, which fail to link on
  # Intel macOS, before that build runs. PROMPT_ALWAYS_RESPOND keeps the
  # pinned sources instead of re-downloading them on non-interactive CI.
  export PROMPT_ALWAYS_RESPOND="${PROMPT_ALWAYS_RESPOND:-n}"
  export PANGROVA_FOLLY_PATCH="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/folly-2026.01.05-openssl-const.patch"
  export PANGROVA_FOLLY_ALIGNMENT_PATCH="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/folly-2026.01.05-spookyhash-alignment.patch"
  local folly_setup_patch="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-folly-common-build.patch"
  local folly_alignment_setup_patch="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-folly-alignment-build.patch"
  if ! git -C "$VELOX_DIR" apply --reverse --check "$folly_alignment_setup_patch" >/dev/null 2>&1; then
    if ! git -C "$VELOX_DIR" apply --reverse --check "$folly_setup_patch" >/dev/null 2>&1; then
      git -C "$VELOX_DIR" apply "$folly_setup_patch"
    fi
    git -C "$VELOX_DIR" apply "$folly_alignment_setup_patch"
  fi
  # Darwin supplies these atomics through its toolchain; preserve sanitizer flags
  # on platforms that do require the separate library.
  local atomic_patch="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-darwin-atomic-link.patch"
  if ! git apply --reverse --check "$atomic_patch" >/dev/null 2>&1; then
    git apply --check "$atomic_patch"
    git apply "$atomic_patch"
  fi
  if [[ "$OS" == "Darwin" ]]; then
    export PROMPT_ALWAYS_RESPOND="${PROMPT_ALWAYS_RESPOND:-Y}"
    export CURL_OPTIONS="${CURL_OPTIONS:---retry 20 --retry-all-errors --connect-timeout 30}"
    source ./scripts/setup-macos.sh
    source "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/resolve-darwin-libevent.sh"
    # Pangrova suppresses Velox's blanket Homebrew install below. Restore the
    # existing Folly prerequisite even when the compiled dependencies are cached.
    libevent_prefix="$(resolve_darwin_libevent "$PANGROVA_BREW" install)"
    local folly_alignment_digest
    folly_alignment_digest=$(shasum -a 256 "$PANGROVA_FOLLY_ALIGNMENT_PATCH" | awk '{print $1}')
    local fbthrift_stamp="$INSTALL_PREFIX/.pangrova-fbthrift-ready"

    # Upstream stores archives inside source directories that it deletes on retry.
    apply_folly_source_patches() {
      local source="$1"
      local folly_patch
      for folly_patch in \
        "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/folly-2026.01.05-openssl-asn1.patch" \
        "$PANGROVA_FOLLY_PATCH" \
        "$PANGROVA_FOLLY_ALIGNMENT_PATCH" \
        "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/folly-2026.01.05-libcxx-exception-path.patch"; do
        if ! patch --dry-run --batch --forward --reverse -d "$source" -p1 \
            -i "$folly_patch" >/dev/null 2>&1; then
          patch --dry-run --batch --forward -d "$source" -p1 -i "$folly_patch"
          patch --batch --forward -d "$source" -p1 -i "$folly_patch"
        fi
      done
    }

    dependency_source_patch() {
      case "$1" in
        wangle)
          printf '%s\n' "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/wangle-2026.01.05-openssl-const.patch" ;;
        fizz)
          printf '%s\n' "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/fizz-2026.01.05-openssl-const.patch" ;;
      esac
    }

    apply_dependency_source_patches() {
      local dir="$1"
      local source="$DEPENDENCY_DIR/$dir"
      local dependency_patch
      while IFS= read -r dependency_patch; do
        [[ -n "$dependency_patch" ]] || continue
        if ! patch --dry-run --batch --forward --reverse -d "$source" -p1 \
            -i "$dependency_patch" >/dev/null 2>&1; then
          patch --dry-run --batch --forward -d "$source" -p1 -i "$dependency_patch"
          patch --batch --forward -d "$source" -p1 -i "$dependency_patch"
        fi
      done < <(dependency_source_patch "$dir")
    }

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
      apply_dependency_source_patches "$dir"
    }

    install_arrow() {
      # Arrow only builds its bundled thrift when ARROW_WITH_THRIFT is on.
      # Thrift's SSL transport no longer compiles against OpenSSL 3.5+ const
      # return types and Parquet is off, so nothing in this install uses it.
      wget_and_untar \
        https://github.com/apache/arrow/archive/apache-arrow-"${ARROW_VERSION}".tar.gz arrow
      (
        if [[ -z "$VELOX_ARROW_CMAKE_PATCH" ]]; then
          ABSOLUTE_SCRIPTDIR=$(realpath "$SCRIPT_DIR")
          VELOX_ARROW_CMAKE_PATCH="$ABSOLUTE_SCRIPTDIR/../CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch"
        fi
        cd "$DEPENDENCY_DIR"/arrow || exit 1
        git apply "$VELOX_ARROW_CMAKE_PATCH"
        if [[ -n $EXTRA_ARROW_PATCH ]]; then
          git apply "$EXTRA_ARROW_PATCH"
        fi
      ) || exit 1
      cmake_install_dir arrow/cpp \
        -DARROW_PARQUET=OFF \
        -DARROW_WITH_THRIFT=OFF \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_JEMALLOC=OFF \
        -DARROW_SIMD_LEVEL=NONE \
        -DARROW_RUNTIME_SIMD_LEVEL=NONE \
        -DARROW_WITH_UTF8PROC=OFF \
        -DARROW_TESTING=OFF \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
        -DCMAKE_BUILD_TYPE=Release \
        -DARROW_BUILD_STATIC=ON \
        -DBOOST_ROOT="$INSTALL_PREFIX" \
        $EXTRA_ARROW_OPTIONS
    }

    install_fizz() {
      # The examples target (BogoShim) links libc++abi symbols that macOS 13
      # does not provide; Velox needs the library, not the developer tools.
      wget_and_untar \
        "https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        fizz
      cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF
    }

    install_fbthrift() {
      wget_and_untar \
        "https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        fbthrift
      # fbthrift's CMake requires an Xxhash package that Velox's dependency set
      # does not install on macOS.
      if [[ ! -f "$INSTALL_PREFIX/include/xxhash.h" ]]; then
        wget_and_untar \
          "https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.8.2.tar.gz" xxHash
        cmake_install_dir xxHash/cmake_unofficial \
          -DBUILD_SHARED_LIBS=OFF -DXXHASH_BUILD_XXHSUM=OFF
      fi
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
      # fbthrift's FindOpenSSL wrapper runs brew unless the CMake variable is set.
      cmake_install_dir fbthrift \
        "-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX" \
        -Denable_tests=OFF -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
      printf '%s\n' openssl@3 > "$fbthrift_stamp"
    }

    install_boost() {
      # Velox and Folly do not use Boost.Log, and its ASIO backend fails to link
      # against the SDK libc++ with Homebrew Clang on Intel. Install all other
      # pinned Boost libraries.
      wget_and_untar \
        https://github.com/boostorg/boost/releases/download/"${BOOST_VERSION}"/"${BOOST_VERSION}".tar.gz boost
      (
        cd "$DEPENDENCY_DIR/boost" || exit 1
        ./bootstrap.sh --prefix="$INSTALL_PREFIX"
        ${SUDO:-} ./b2 "-j${NPROC}" -d0 install threading=multi --without-python \
          --without-log
      )
    }

    install_folly() {
      wget_and_untar \
        "https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        folly
      apply_folly_source_patches "$DEPENDENCY_DIR/folly"
      # Release consumers instantiate Folly's inline templates with NDEBUG.
      # Reconfigure generated Folly builds before replacing an older archive.
      PROMPT_ALWAYS_RESPOND=y cmake_install_dir folly \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS="$VELOX_BUILD_SHARED" -DBUILD_TESTS=OFF \
        -DFOLLY_HAVE_INT128_T=ON -DGFLAGS_SHARED=FALSE \
        "-DCMAKE_PREFIX_PATH=$INSTALL_PREFIX;$libevent_prefix;$HOMEBREW_PREFIX"
      if [[ "$VELOX_BUILD_SHARED" != "ON" ]]; then
        [[ -f "$INSTALL_PREFIX/lib/libfolly.a" ]] || die "Darwin Folly archive was not installed"
        grep -Fxq 'CMAKE_BUILD_TYPE:STRING=Release' "$DEPENDENCY_DIR/folly/_build/CMakeCache.txt" \
          || die "Darwin Folly archive was not built in Release mode"
        local stamp
        stamp=$(mktemp "$INSTALL_PREFIX/.pangrova-folly-alignment.XXXXXXXX")
        printf 'Release %s\n' "$folly_alignment_digest" > "$stamp"
        mv "$stamp" "$INSTALL_PREFIX/.pangrova-folly-alignment"
      fi
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
    # Hive/DWIO needs a CMake package and a static archive for the JNI link.
    ensure_darwin_snappy "$INSTALL_PREFIX"
    BUILD_FAISS=false
    if [[ ! -f "$INSTALL_PREFIX/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]]; then
      run_and_time install_aws_deps
    fi
    if [[ -f "$fbthrift_stamp" ]]; then
      if ! darwin_folly_has_release_archive "$INSTALL_PREFIX" "$folly_alignment_digest"; then
        run_and_time install_folly
      fi
      if [[ "$(<"$fbthrift_stamp")" != openssl@3 ]]; then
        run_and_time install_fbthrift
      fi
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
    if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
      local boost_patch="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-linux-production-pic-boost.patch"
      if ! git apply --reverse --check "$boost_patch" >/dev/null 2>&1; then
        git apply --check "$boost_patch"
        git apply "$boost_patch"
      fi
    fi
    # Pangrova builds Velox without its DuckDB-backed upstream test utilities.
    BUILD_DUCKDB="${BUILD_DUCKDB:-false}" ./scripts/setup-ubuntu.sh
    if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
      # Force-load every object of the selected archives into a shared
      # library before spending the Velox build on unusable dependencies.
      local boost_lib="${INSTALL_PREFIX:-/usr/local}/lib"
      "$CXX" -shared -Wl,-z,defs,-z,text -Wl,--whole-archive \
        "$boost_lib/libboost_filesystem.a" "$boost_lib/libboost_atomic.a" \
        -Wl,--no-whole-archive -pthread -o "$WORK_DIR/boost-pic-check.so"
      rm "$WORK_DIR/boost-pic-check.so"
    fi
    # setup-ubuntu.sh installs core Velox dependencies but leaves cloud adapters
    # to an explicit command. The S3-enabled Velox configure requires AWSSDK.
    if [[ ! -f "${INSTALL_PREFIX:-/usr/local}/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]]; then
      ./scripts/setup-ubuntu.sh install_aws_deps
    fi
  fi

  ensure_folly_compiler_abi "$libevent_prefix"
}

verify_prepared_velox_dependencies() {
  # Used only after a same-run stage receipt and archive have been verified.
  [[ "$OS" == Darwin && "$SKIP_DEPS" == 1 ]] \
    || die "prepared-velox requires a verified macOS dependency prefix (SKIP_DEPS=1)"
  [[ -d "$VELOX_DIR/.git" ]] || die "Prepared Velox source checkout is missing"
  cd "$VELOX_DIR"
  bash "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/apply-velox-patches.sh" verify "$VELOX_DIR"
  # The alignment patch modifies the common patch's hunk, so validate the
  # final alignment patch rather than reverse-checking the intermediate patch.
  local patch_name
  for patch_name in velox-folly-alignment-build.patch velox-darwin-atomic-link.patch; do
    git apply --reverse --check "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/$patch_name" \
      || die "Prepared Velox source is missing $patch_name"
  done
  # Recreate the same upstream macOS CMake environment the producer used.
  # Sourcing defines the toolchain variables and functions, not install tasks.
  local GRPC_VERSION="$GRPC_VERSION" PROTOBUF_VERSION="$PROTOBUF_VERSION" ABSEIL_VERSION="$ABSEIL_VERSION"
  export PROMPT_ALWAYS_RESPOND="${PROMPT_ALWAYS_RESPOND:-n}"
  export CURL_OPTIONS="${CURL_OPTIONS:---retry 20 --retry-all-errors --connect-timeout 30}"
  source ./scripts/setup-macos.sh
  [[ "$INSTALL_PREFIX" == "$VELOX_DIR/deps-install" ]] || die "Prepared Velox prefix differs"
  local folly_alignment_digest
  folly_alignment_digest="$(shasum -a 256 "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/folly-2026.01.05-spookyhash-alignment.patch" | awk '{print $1}')"
  darwin_folly_has_release_archive "$INSTALL_PREFIX" "$folly_alignment_digest" \
    || die "Prepared Folly archive lacks the required Release and alignment patch identity"
  [[ -f "$INSTALL_PREFIX/.pangrova-fbthrift-ready" && \
      "$(< "$INSTALL_PREFIX/.pangrova-fbthrift-ready")" == openssl@3 && \
      -f "$INSTALL_PREFIX/lib/cmake/Snappy/SnappyConfig.cmake" && \
      -f "$INSTALL_PREFIX/lib/libsnappy.a" && \
      -f "$INSTALL_PREFIX/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]] \
    || die "Prepared Velox dependency installation is incomplete"
  source "$PANGROVA_BOOTSTRAP_SCRIPT_DIR/resolve-darwin-libevent.sh"
  local libevent_prefix
  libevent_prefix="$(resolve_darwin_libevent "$PANGROVA_BREW" check)"
  ensure_folly_compiler_abi "$libevent_prefix"
  step "Verified prepared macOS Velox prerequisites"
}

build_velox() {
  log "Building Velox $VELOX_REF"
  if [[ "$PANGROVA_NATIVE_BUILD_PHASE" == "prepared-velox" ]]; then
    verify_prepared_velox_dependencies
  else
    prepare_velox_dependencies
  fi
  export PATH="$(dirname "$PANGROVA_CMAKE_BIN"):$PATH"
  if [[ "$PANGROVA_NATIVE_BUILD_PHASE" == "dependencies" ]]; then
    step "Velox dependency sources and installation are ready"
    return
  fi

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
  local velox_platform_args=()
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "ON" ]]; then
    velox_platform_args+=(-DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
    velox_platform_args+=(-DCMAKE_FIND_LIBRARY_SUFFIXES=.a -DOPENSSL_USE_STATIC_LIBS=TRUE)
  fi
  local velox_prefix_path="$DEPS_PREFIX;$VELOX_DIR/deps-install"
  if [[ "$OS" == "Darwin" ]]; then
    velox_platform_args+=("-DSnappy_DIR=$INSTALL_PREFIX/lib/cmake/Snappy")
    velox_platform_args+=("-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX")
    velox_prefix_path+=";$OPENSSL_PREFIX"
    [[ -n "$FLEX_PREFIX" ]] && velox_platform_args+=("-DFLEX_INCLUDE_DIR=$FLEX_PREFIX/include")
    [[ -n "$HOMEBREW_PREFIX" ]] && velox_prefix_path+=";$HOMEBREW_PREFIX"
  fi
  cmake -B _build/release -GNinja $CMAKE_LAUNCHER_FLAGS \
    -DPANGROVA_FUNCTION_SELECTION_TOOLS="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../../pangrova-protocol/tools/function-contracts" \
    -DPANGROVA_FUNCTION_SELECTION_DECLARATION="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../../pangrova-protocol/src/main/resources/function-contracts/execution-selection.json" \
    -DPANGROVA_FUNCTION_SDK_PATCH_FILE="$PANGROVA_BOOTSTRAP_SCRIPT_DIR/../patches/velox-function-metadata-snapshot.patch" \
    -DPython3_EXECUTABLE="${PANGROVA_FUNCTION_CONTRACT_PYTHON:-python3}" \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="$velox_prefix_path" \
    ${velox_platform_args[@]+"${velox_platform_args[@]}"} \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_POLICY_DEFAULT_CMP0167=NEW \
    -Dsimdjson_SOURCE=BUNDLED \
    -DVELOX_MONO_LIBRARY=ON \
    -Dabsl_DIR:PATH="$DEPS_PREFIX/lib/cmake/absl" \
    -Dre2_DIR:PATH="$DEPS_PREFIX/lib/cmake/re2" \
    -DVELOX_BUILD_TESTING=OFF \
    -DVELOX_BUILD_TEST_UTILS=OFF \
    -DVELOX_ENABLE_DUCKDB=OFF \
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
  # Velox builds Arrow core but does NOT enable Flight.
  # We build IPC (including its declared codecs) and Flight from the same source
  # to ensure ABI compatibility (same Arrow version, same compiler flags).
  # Velox's current dependency installer checks out Arrow under deps-download;
  # older configured builds used the ExternalProject source tree instead.
  local arrow_src="$VELOX_DIR/deps-download/arrow"
  if [[ ! -f "$arrow_src/cpp/CMakeLists.txt" ]]; then
    arrow_src="$VELOX_DIR/_build/release/CMake/resolve_dependency_modules/arrow/arrow_ep/src/arrow_ep"
  fi
  if [[ ! -f "$arrow_src/cpp/CMakeLists.txt" ]]; then
    die "Arrow source not found in Velox's deps-download or ExternalProject tree — build Velox first"
  fi

  step "Building libarrow_flight from $arrow_src"
  local arrow_static=OFF arrow_shared=ON arrow_target=arrow_flight_shared arrow_parquet=OFF
  local arrow_build="$WORK_DIR/arrow-flight-build"
  local arrow_platform_args=()
  if [[ "$OS" == "Darwin" ]]; then
    [[ -n "$OPENSSL_PREFIX" ]] && arrow_platform_args+=("-DOPENSSL_ROOT_DIR=$OPENSSL_PREFIX")
    # Use Arrow's checksummed codec source so its static archive and headers share a prefix.
    arrow_platform_args+=("-Dzstd_SOURCE=BUNDLED")
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
    ${arrow_platform_args[@]+"${arrow_platform_args[@]}"} \
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
export BOOST_ROOT="VELOX_DIR_VAL/deps-install"
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
    llvm_env="export CC=\"$CC\"\nexport CXX=\"$CXX\"\nexport SDKROOT=\"$SDKROOT\"\nexport MACOSX_DEPLOYMENT_TARGET=\"$MACOSX_DEPLOYMENT_TARGET\"\nexport OPENSSL_ROOT_DIR=\"$OPENSSL_PREFIX\"\nexport PATH=\"$(dirname "$CC"):\$PATH\""
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
  if [[ "$SKIP_VELOX" != "1" && "$PANGROVA_NATIVE_BUILD_PHASE" != "dependencies" ]] && \
     [[ ! -f "$VELOX_DIR/_build/release/lib/libvelox.a" ]] && \
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
  # Arrow's retired JFrog mirror can be the only Snappy URL selected on macOS.
  # Use the byte-identical upstream tag archive and keep Arrow's SHA-256 check.
  export ARROW_SNAPPY_URL="${ARROW_SNAPPY_URL:-https://codeload.github.com/google/snappy/tar.gz/refs/tags/1.1.10}"
  if [[ "$PANGROVA_PRODUCTION_BUILD" == "OFF" ]]; then
    # Arrow still verifies the pinned archive's SHA-256 in its ExternalProject.
    export ARROW_LZ4_URL="${ARROW_LZ4_URL:-https://codeload.github.com/lz4/lz4/tar.gz/refs/tags/v1.10.0}"
  fi
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
    case "$PANGROVA_NATIVE_BUILD_PHASE" in
      all|prepared-velox)
        build_velox
        build_arrow_flight
        ;;
      dependencies|velox) build_velox ;;
      arrow-flight) build_arrow_flight ;;
      *) die "PANGROVA_NATIVE_BUILD_PHASE must be all, dependencies, velox, arrow-flight, or prepared-velox" ;;
    esac
  else
    step "Skipping Velox build (SKIP_VELOX=1)"
  fi

  write_env
  verify
  log "Setup complete — source $PANGROVA_HOME/env.sh to activate"
}

main "$@"
