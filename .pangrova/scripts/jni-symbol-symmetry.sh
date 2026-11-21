#!/usr/bin/env bash
# Verify that the packaged pangrova_native library exports only JNI entry points
# and that every Java native declaration has exactly one exported short JNI name.
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAVA_ROOTS=(
    "${ROOT_DIR}/pangrova-worker/src/main/java"
    "${ROOT_DIR}/pangrova-ai/pangrova-ai-worker/src/main/java"
)
for java_root in "${JAVA_ROOTS[@]}"; do
    if [[ ! -d "${java_root}" ]]; then
        echo "FAIL: Java native-declaration root is missing: ${java_root}" >&2
        exit 1
    fi
done

if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [path-to-libpangrova_native]" >&2
    exit 2
fi

if [[ $# -eq 1 ]]; then
    NATIVE_LIB="$1"
else
    NATIVE_LIB=""
    # The build redirects CMAKE_LIBRARY_OUTPUT_DIRECTORY into the SDK cohort, so
    # honour the configured output directory before falling back to the build root.
    BUILD_DIR="${PANGROVA_NATIVE_BUILD_DIR:-${ROOT_DIR}/pangrova-worker-native/build}"
    LIB_DIR="${BUILD_DIR}"
    if [[ -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
        configured_dir="$(sed -n 's/^CMAKE_LIBRARY_OUTPUT_DIRECTORY:[A-Z]*=//p' "${BUILD_DIR}/CMakeCache.txt" | head -n 1)"
        if [[ -n "${configured_dir}" ]]; then
            LIB_DIR="${configured_dir}"
        fi
    fi
    for candidate in \
        "${LIB_DIR}/libpangrova_native.dylib" \
        "${LIB_DIR}/libpangrova_native.so" \
        "${BUILD_DIR}/libpangrova_native.dylib" \
        "${BUILD_DIR}/libpangrova_native.so"; do
        if [[ -f "${candidate}" ]]; then
            NATIVE_LIB="${candidate}"
            break
        fi
    done
    if [[ -z "${NATIVE_LIB}" ]]; then
        echo "FAIL: libpangrova_native is not built" >&2
        exit 1
    fi
fi

if [[ ! -f "${NATIVE_LIB}" ]]; then
    echo "FAIL: native library not found: ${NATIVE_LIB}" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT
EXPECTED="${TMP_DIR}/expected"
ACTUAL_JAVA="${TMP_DIR}/actual-java"
EXPORTED="${TMP_DIR}/exported"

# Strip comments before parsing so documentation containing the word "native"
# cannot create a false declaration. Pangrova currently has no overloaded JNI
# methods; fail if one is introduced because overloaded names require a signature
# suffix and should be generated with javac -h instead of guessed here.
while IFS= read -r java_file; do
    perl -0777 -e '
        my $source = do { local $/; <> };
        $source =~ s{/\*.*?\*/}{}gs;
        $source =~ s{//[^\n]*}{}g;
        my ($package) = $source =~ /\bpackage\s+([A-Za-z0-9_.]+)\s*;/;
        my ($class) = $source =~ /\bpublic\s+(?:final\s+)?class\s+([A-Za-z0-9_]+)/;
        exit 0 unless defined $package && defined $class;
        my %count;
        while ($source =~ /\bnative\s+[A-Za-z0-9_.$<>?\[\]]+\s+([A-Za-z0-9_]+)\s*\(/g) {
            $count{$1}++;
        }
        for my $method (sort keys %count) {
            die "overloaded JNI method requires javac -h: $package.$class.$method\n"
                if $count{$method} > 1;
            my $owner = "$package.$class";
            $owner =~ s/_/_1/g;
            $owner =~ s/\./_/g;
            (my $encoded_method = $method) =~ s/_/_1/g;
            print "Java_${owner}_${encoded_method}\n";
        }
    ' "${java_file}"
done < <(find "${JAVA_ROOTS[@]}" -type f -name '*.java' -print | sort) \
    | sort -u > "${EXPECTED}"

case "$(uname -s)" in
    Darwin)
        nm -gU "${NATIVE_LIB}" | awk '{print $3}' | sed 's/^_//' | sort -u > "${EXPORTED}"
        ;;
    Linux)
        nm -D --defined-only "${NATIVE_LIB}" | awk '{print $3}' | sort -u > "${EXPORTED}"
        ;;
    *)
        echo "FAIL: unsupported platform for symbol inspection: $(uname -s)" >&2
        exit 1
        ;;
esac

grep '^Java_' "${EXPORTED}" | sort -u > "${ACTUAL_JAVA}" || true

if grep -Ev '^(Java_|JNI_OnLoad$|JNI_OnUnload$)' "${EXPORTED}" > "${TMP_DIR}/unexpected-exports"; then
    echo "FAIL: non-JNI global symbols are exported:" >&2
    head -n 50 "${TMP_DIR}/unexpected-exports" >&2
    exit 1
fi

comm -23 "${EXPECTED}" "${ACTUAL_JAVA}" > "${TMP_DIR}/missing"
comm -13 "${EXPECTED}" "${ACTUAL_JAVA}" > "${TMP_DIR}/orphan"
if [[ -s "${TMP_DIR}/missing" || -s "${TMP_DIR}/orphan" ]]; then
    if [[ -s "${TMP_DIR}/missing" ]]; then
        echo "FAIL: Java native declarations missing from the library:" >&2
        cat "${TMP_DIR}/missing" >&2
    fi
    if [[ -s "${TMP_DIR}/orphan" ]]; then
        echo "FAIL: orphan Java_* exports without Java declarations:" >&2
        cat "${TMP_DIR}/orphan" >&2
    fi
    exit 1
fi

if ! grep -qx 'JNI_OnLoad' "${EXPORTED}"; then
    echo "FAIL: JNI_OnLoad is not exported" >&2
    exit 1
fi

echo "PASS: JNI symbols are isolated and symmetric ($(wc -l < "${ACTUAL_JAVA}" | tr -d ' ') methods)"
