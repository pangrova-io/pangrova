#!/usr/bin/env bash
# Usage: stage-java-native-test-runtime.sh <Linux Native build dir> <staging dir>
set -euo pipefail

build_dir="$1"
stage_dir="$2"
fixture="$build_dir/pangrova_lance_fixture_tool"
mkdir -p -- "$stage_dir/lib" "$stage_dir/libexec"
install -m 755 -- "$fixture" "$stage_dir/libexec/pangrova_lance_fixture_tool"

for binary in "$fixture" "$build_dir/libpangrova_native.so"; do
    dependencies=$(LC_ALL=C ldd "$binary")
    while read -r soname arrow resolution; do
        if [[ "$arrow" != '=>' ]]; then
            case "$soname" in
                /*/ld-linux-*.so.*) ;;
                /*)
                    printf 'Absolute dependency %s cannot relocate with %s\n' "$soname" "$binary" >&2
                    exit 1
                    ;;
            esac
            continue
        fi
        library=${resolution% (*}
        if [[ "$library" != /* || ! -f "$library" ]]; then
            printf 'Unresolved dependency %s for %s\n' "$soname" "$binary" >&2
            exit 1
        fi
        if [[ "$soname" == */* ]]; then
            printf 'Invalid library name %s for %s\n' "$soname" "$binary" >&2
            exit 1
        fi
        # The producer and consumers use the same Ubuntu version. Keep the
        # loader, glibc, and compiler runtime from the destination system.
        case "$soname" in
            libc.so.6|libm.so.6|libpthread.so.0|libdl.so.2|librt.so.1|libresolv.so.2|\
            libstdc++.so.6|libgcc_s.so.1) continue ;;
        esac
        destination="$stage_dir/lib/$soname"
        if [[ -e "$destination" ]]; then
            if ! cmp -s -- "$library" "$destination"; then
                printf 'Native test runtime has incompatible copies of %s\n' "$soname" >&2
                exit 1
            fi
        else
            cp -L -- "$library" "$destination"
        fi
    done <<< "$dependencies"
done

# Scope the fixture libraries to this process when Maven uses production JNI.
cat > "$stage_dir/pangrova_lance_fixture_tool" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
runtime_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
export LD_LIBRARY_PATH="$runtime_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec "$runtime_dir/libexec/pangrova_lance_fixture_tool" "$@"
SH
chmod +x "$stage_dir/pangrova_lance_fixture_tool"
