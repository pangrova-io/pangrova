#!/usr/bin/env bash
# Sourced by the Darwin native bootstraps. Folly's finder needs both files.
resolve_darwin_libevent() {
  local brew_bin="$1" mode="${2:-check}" prefix
  prefix="$("$brew_bin" --prefix libevent 2>/dev/null || true)"
  if [[ "$mode" == install ]] && ! darwin_libevent_present "$prefix"; then
    HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 \
      "$brew_bin" install --formula libevent >&2 || die "Homebrew could not install libevent"
    prefix="$("$brew_bin" --prefix libevent 2>/dev/null || true)"
  fi
  darwin_libevent_present "$prefix" \
    || die "Homebrew libevent headers or library not found; run setup-native-build-env.sh"
  printf '%s\n' "$prefix"
}

darwin_libevent_present() {
  local prefix="$1"
  [[ -n "$prefix" && -f "$prefix/include/event.h" && \
     ( -f "$prefix/lib/libevent.dylib" || -f "$prefix/lib/libevent.a" ) ]]
}
