#!/usr/bin/env bash
# Build the pinned Boost source with the same libc++ headers and runtime.
set -euo pipefail

[[ "$(uname -s)" == Darwin && "$(uname -m)" == x86_64 ]] || {
  echo "ERROR: this Boost adapter is only for macOS x86_64" >&2
  exit 1
}
[[ -n "${CXX:-}" && "$CXX" == /* && -x "$CXX" ]] || {
  echo "ERROR: an absolute CXX from the selected Darwin toolchain is required" >&2
  exit 1
}
sdk_include="${SDKROOT:?Select the Xcode SDK with SDKROOT}/usr/include/c++/v1"
[[ -f "$sdk_include/exception" ]] || {
  echo "ERROR: selected macOS SDK libc++ headers are missing: $sdk_include" >&2
  exit 1
}
# B2 auto-selects clang from PATH, as the pinned Velox bootstrap does.
export PATH="${CXX%/*}:$PATH"

case "${1:-}" in
  --check-runtime)
    probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/pangrova-cxx-abi.XXXXXX")"
    trap 'rm -rf -- "$probe_dir"' EXIT
    cat > "$probe_dir/probe.cpp" <<'CPP'
#include <exception>
#include <stdexcept>

int main() {
    std::exception_ptr captured = std::make_exception_ptr(std::runtime_error("probe"));
    try {
        std::rethrow_exception(captured);
    } catch (const std::runtime_error&) {
        return 0;
    }
    return 1;
}
CPP
    if ! "$CXX" -std=c++11 -nostdinc++ -isysroot "$SDKROOT" \
         -isystem "$sdk_include" "$probe_dir/probe.cpp" -o "$probe_dir/probe"; then
      echo "ERROR: selected macOS SDK libc++ link probe failed for the Boost build" >&2
      exit 1
    fi
    dylibs="$(otool -L "$probe_dir/probe")"
    if ! grep -Eq '^[[:space:]]+/usr/lib/libc\+\+\.1\.dylib[[:space:]]' <<< "$dylibs"; then
      echo "ERROR: macOS Boost probe must link the system libc++, not a bundled runtime" >&2
      exit 1
    fi
    if ! "$probe_dir/probe"; then
      echo "ERROR: selected macOS SDK libc++ runtime probe failed for the Boost build" >&2
      exit 1
    fi
    ;;
  --build)
    [[ $# == 3 && "$3" =~ ^[1-9][0-9]*$ ]] || {
      echo "ERROR: expected --build INSTALL_PREFIX JOBS" >&2
      exit 1
    }
    # B2 emits these feature values into its shell command. It normalizes
    # backslashes, so Bash %q does not preserve paths containing spaces.
    # Literal single quotes work; reject paths that would escape them.
    case "$SDKROOT" in
      *"'"*|*'\'*|*$'\n'*|*$'\r'*)
        echo "ERROR: macOS SDK path contains unsupported shell characters" >&2
        exit 1
        ;;
    esac
    ./bootstrap.sh --prefix="$2" --with-toolset=clang
    # Preserve the pinned Velox installer arguments and the optional sudo launcher.
    ${SUDO:-} ./b2 "-j$3" -d0 install threading=multi --without-python toolset=clang \
      "cxxflags=-nostdinc++ -isysroot '$SDKROOT' -isystem '$sdk_include'" \
      "linkflags=-isysroot '$SDKROOT'"
    ;;
  --check-log)
    [[ $# == 2 && -f "$2/lib/libboost_log.dylib" ]] || {
      echo "ERROR: the installed Intel Boost.Log dynamic library is required" >&2
      exit 1
    }
    if ! otool -L "$2/lib/libboost_log.dylib" |
        grep -E '^[[:space:]]+/usr/lib/libc\+\+\.1\.dylib[[:space:]]' >/dev/null; then
      echo "ERROR: installed Intel Boost.Log must link the system libc++" >&2
      exit 1
    fi
    probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/pangrova-boost-log.XXXXXX")"
    trap 'rm -rf -- "$probe_dir"' EXIT
    cat > "$probe_dir/probe.cpp" <<'CPP'
#include <boost/log/core.hpp>

int main() {
    boost::log::core::get()->set_logging_enabled(false);
}
CPP
    "$CXX" -std=c++11 -nostdinc++ -isysroot "$SDKROOT" -isystem "$sdk_include" \
      -DBOOST_LOG_DYN_LINK -I"$2/include" "$probe_dir/probe.cpp" \
      -L"$2/lib" -Wl,-rpath,"$2/lib" -lboost_log -o "$probe_dir/probe"
    "$probe_dir/probe"
    ;;
  *)
    echo "Usage: $0 --check-runtime | --build INSTALL_PREFIX JOBS | --check-log INSTALL_PREFIX" >&2
    exit 2
    ;;
esac
