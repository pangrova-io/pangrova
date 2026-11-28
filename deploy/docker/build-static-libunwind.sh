#!/usr/bin/env bash
# Build the pinned libunwind archives as PIC for the production JNI library.
# Ubuntu's static libunwind is built with non-PIC LTO and cannot be linked into
# an AArch64 shared object (R_AARCH64_ADR_PREL_PG_HI21 on __stack_chk_guard).
set -euo pipefail

version=1.6.2
sha256=4a6aec666991fb45d0889c44aede8ad6eb108071c3554fcdff671f9c94794976
prefix="${PANGROVA_DEPS_PREFIX:?PANGROVA_DEPS_PREFIX is required}"
work_dir="$(mktemp -d)"
trap 'rm -rf -- "$work_dir"' EXIT
archive="$work_dir/libunwind-${version}.tar.gz"

curl --http1.1 --proto '=https' --tlsv1.2 -fsSL --retry 6 --retry-all-errors \
    --connect-timeout 30 --max-time 300 \
    "https://download.savannah.nongnu.org/releases/libunwind/libunwind-${version}.tar.gz" \
    -o "$archive"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$work_dir"

mkdir "$work_dir/build"
cd "$work_dir/build"
CFLAGS='-O2 -fPIC -fno-lto' \
    "$work_dir/libunwind-${version}/configure" \
    --prefix="$prefix" \
    --disable-shared --enable-static \
    --disable-tests --disable-coredump --disable-ptrace --disable-setjmp
make --jobs="${CMAKE_BUILD_PARALLEL_LEVEL:-1}"
make install

arch="$(uname -m)"
for archive_name in libunwind.a "libunwind-${arch}.a"; do
    test -s "$prefix/lib/$archive_name" || {
        echo "Missing PIC libunwind archive: $prefix/lib/$archive_name" >&2
        exit 1
    }
done

# Match the production JNI link group. On x86-64, the architecture archive
# references _Ux86_64_setcontext in libunwind.a after the first archive scan.
# The link also catches non-PIC objects before the much larger Native link.
cat > "$work_dir/pic-check.c" <<'EOF'
#include <libunwind.h>
int pangrova_libunwind_pic_check(void) {
    unw_context_t context;
    unw_cursor_t cursor;
    unw_getcontext(&context);
    unw_init_local(&cursor, &context);
    return unw_step(&cursor);
}
EOF
"${CC:-cc}" -fPIC -shared -Wl,-z,defs \
    -I"$prefix/include" "$work_dir/pic-check.c" \
    -Wl,--start-group "$prefix/lib/libunwind.a" "$prefix/lib/libunwind-${arch}.a" \
    -Wl,--end-group \
    -Wl,-Bstatic -lz -llzma -Wl,-Bdynamic -ldl -lm \
    -o "$work_dir/libunwind-pic-check.so"
if readelf -d "$work_dir/libunwind-pic-check.so" | grep -F 'libunwind.so' >/dev/null; then
    echo 'Static libunwind PIC check unexpectedly requires shared libunwind' >&2
    exit 1
fi
