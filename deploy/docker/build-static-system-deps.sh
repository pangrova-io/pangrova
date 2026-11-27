#!/usr/bin/env bash
set -euo pipefail

: "${PANGROVA_DEPS_PREFIX:?Set the production native dependency prefix}"
[[ $# == 1 ]] || { echo "Usage: $0 JNI_VERSION_MAP" >&2; exit 1; }
version_map="$1"
test -s "$version_map"
prefix="$PANGROVA_DEPS_PREFIX"
jobs="${CMAKE_BUILD_PARALLEL_LEVEL:-1}"
work=$(mktemp -d)
trap 'rm -r "$work"' EXIT
chmod 755 "$work"

# Fetch the exact Ubuntu source versions corresponding to the installed
# development packages. APT verifies their signed index and content hashes.
sed -n -E 's/^deb[[:space:]]+/deb-src /p' /etc/apt/sources.list > "$work/sources.list"
test -s "$work/sources.list"
mkdir "$work/lists"
apt_sources=(
  -o "Dir::Etc::sourcelist=$work/sources.list"
  -o Dir::Etc::sourceparts=-
  -o "Dir::State::lists=$work/lists"
)
apt-get "${apt_sources[@]}" update

download_source() (
  package="$1"
  reference=$(dpkg-query -W -f='${source:Package}=${source:Version}' "$package")
  mkdir "$work/$package"
  cd "$work/$package"
  chown _apt .
  apt-get "${apt_sources[@]}" source --only-source "$reference" >&2
  sources=(*/)
  [[ ${#sources[@]} == 1 && -d "${sources[0]}" ]]
  printf '%s/%s\n' "$PWD" "${sources[0]%/}"
)

export PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
mkdir -p "$prefix/lib" "$prefix/include"

bzip2=$(download_source libbz2-dev)
make -C "$bzip2" -j "$jobs" CC="${CC:-cc}" \
  CFLAGS="-O2 -fPIC -D_FILE_OFFSET_BITS=64" libbz2.a
install -m 644 "$bzip2/libbz2.a" "$prefix/lib/"
install -m 644 "$bzip2/bzlib.h" "$prefix/include/"

zstd=$(download_source libzstd-dev)
make -C "$zstd/lib" -j "$jobs" CC="${CC:-cc}" MOREFLAGS=-fPIC libzstd.a-mt
make -C "$zstd/lib" PREFIX="$prefix" install-static install-includes install-pc

libevent=$(download_source libevent-dev)
cmake -S "$libevent" -B "$work/libevent" -GNinja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
  -DEVENT__LIBRARY_TYPE=STATIC -DEVENT__DISABLE_TESTS=ON \
  -DEVENT__DISABLE_REGRESS=ON -DEVENT__DISABLE_BENCHMARK=ON -DEVENT__DISABLE_SAMPLES=ON
cmake --build "$work/libevent" --parallel "$jobs"
cmake --install "$work/libevent"

icu=$(download_source libicu-dev)
(
  cd "$icu/source"
  CFLAGS="-O2 -fPIC" CXXFLAGS="-O2 -fPIC" ./configure \
    --prefix="$prefix" --libdir="$prefix/lib" --disable-shared --enable-static \
    --with-data-packaging=static --disable-tests --disable-samples
  make -j "$jobs"
  make install
)

libxml2=$(download_source libxml2-dev)
(
  cd "$libxml2"
  # xmllint and xmlcatalog use a C linker. ICU's static archives need the C++ runtime.
  CFLAGS="-O2 -fPIC" LIBS="-lstdc++" ./configure --prefix="$prefix" --libdir="$prefix/lib" \
    --disable-shared --enable-static --with-pic --with-icu --without-python
  make -j "$jobs"
  make install
)

# Use the production export map and check all compiled objects, including
# code paths that the current JNI link may leave out.
archives=()
for name in bz2 zstd event_core event_extra event_openssl event_pthreads \
  icui18n icuuc icudata xml2; do
  archives+=("$prefix/lib/lib$name.a")
done
system_lib="/usr/lib/$("${CC:-cc}" -dumpmachine)"
for name in ssl crypto z lzma; do
  archives+=("$system_lib/lib$name.a")
done
"${CXX:-c++}" -shared -Wl,-z,defs,-z,text -Wl,--version-script="$version_map" \
  -Wl,--whole-archive "${archives[@]}" -Wl,--no-whole-archive \
  -pthread -lm -ldl -lrt -o "$work/system-pic-check.so"
