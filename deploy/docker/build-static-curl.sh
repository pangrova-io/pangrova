#!/usr/bin/env bash
set -euo pipefail

: "${PANGROVA_DEPS_PREFIX:?Set the production native dependency prefix}"

curl_version=8.23.0
curl_sha256=8dadd7d2536a314f982ad05a651b713f426bf7d02cc2c76cffb9796f2218e6c5
work_dir="${PANGROVA_NATIVE_BUILD_DIR:-${TMPDIR:-/tmp}/pangrova-native-build}/curl-source"
archive="$work_dir/curl-$curl_version.tar.xz"
mkdir -p "$work_dir"
curl --http1.1 --proto '=https' --tlsv1.2 -fsSL --retry 6 --retry-all-errors \
  --connect-timeout 30 --max-time 300 \
  "https://curl.se/download/curl-$curl_version.tar.xz" -o "$archive"
printf '%s  %s\n' "$curl_sha256" "$archive" | sha256sum -c -
tar -xf "$archive" -C "$work_dir"

cmake -S "$work_dir/curl-$curl_version" -B "$work_dir/build" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PANGROVA_DEPS_PREFIX" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON -DBUILD_CURL_EXE=OFF \
  -DCURL_USE_OPENSSL=ON -DCURL_USE_GSSAPI=OFF \
  -DCURL_DISABLE_KERBEROS_AUTH=ON -DCURL_DISABLE_NEGOTIATE_AUTH=ON \
  -DCURL_USE_LIBSSH2=OFF -DCURL_USE_LIBSSH=OFF \
  -DCURL_USE_LIBPSL=OFF -DUSE_LIBIDN2=OFF \
  -DCURL_BROTLI=OFF -DCURL_ZSTD=OFF -DCURL_ZLIB=ON \
  -DCURL_DISABLE_LDAP=ON -DCURL_DISABLE_LDAPS=ON -DUSE_NGHTTP2=OFF
cmake --build "$work_dir/build" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-1}"
cmake --install "$work_dir/build"

test -f "$PANGROVA_DEPS_PREFIX/lib/libcurl.a"
test ! -f "$PANGROVA_DEPS_PREFIX/lib/libcurl.so"
export PKG_CONFIG_PATH="$PANGROVA_DEPS_PREFIX/lib/pkgconfig"
pkg-config --modversion libcurl | grep -Fx "$curl_version"

# Link every curl object once so rarely used code cannot carry text relocations.
static_flags=$(pkg-config --static --libs libcurl)
read -r -a curl_flags <<< "$static_flags"
dependencies=()
for flag in "${curl_flags[@]}"; do
  [[ "$flag" == -lcurl ]] || dependencies+=("$flag")
done
"${CC:-cc}" -shared -Wl,-z,defs,-z,text -Wl,--whole-archive \
  "$PANGROVA_DEPS_PREFIX/lib/libcurl.a" -Wl,--no-whole-archive \
  "${dependencies[@]}" -o "$work_dir/curl-pic-check.so"
rm "$work_dir/curl-pic-check.so"
