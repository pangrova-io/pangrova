#!/usr/bin/env bash
# Sourced by the Darwin native bootstraps.

darwin_openssl_present() {
  local prefix="$1"
  [[ -n "$prefix" && -f "$prefix/include/openssl/ssl.h" &&
     -f "$prefix/lib/libssl.a" && -f "$prefix/lib/libcrypto.a" ]]
}

configure_darwin_openssl() {
  local brew_bin="$1" mode="${2:-check}"
  OPENSSL_PREFIX="$("$brew_bin" --prefix openssl@3 2>/dev/null || true)"
  if [[ "$mode" == install ]] && ! darwin_openssl_present "$OPENSSL_PREFIX"; then
    HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 "$brew_bin" install openssl@3 || return
    OPENSSL_PREFIX="$("$brew_bin" --prefix openssl@3)"
  fi
  if ! darwin_openssl_present "$OPENSSL_PREFIX"; then
    printf 'ERROR: Homebrew openssl@3 headers and static libraries are required. Run setup-native-build-env.sh.\n' >&2
    return 1
  fi
  # gRPC still calls the ENGINE API, which OpenSSL 4 removed. Nested CMake
  # projects and pkg-config must use OpenSSL 3 from the first dependency build.
  export OPENSSL_ROOT_DIR="$OPENSSL_PREFIX"
  export PKG_CONFIG_PATH="$OPENSSL_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
}
