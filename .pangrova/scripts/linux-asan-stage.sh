#!/usr/bin/env bash
# Transfer the completed ASan dependency cohort between two Linux CI runners.
set -euo pipefail

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ $# == 2 ]] || die "Usage: $0 pack|restore BUNDLE_DIR"
operation="$1"
bundle="$2"
root="$HOME/.pangrova"
[[ "$(uname -s)" == Linux ]] || die "ASan stage bundles are Linux only"
: "${GITHUB_RUN_ID:?missing GitHub run identity}"
: "${GITHUB_SHA:?missing revision identity}"
: "${GITHUB_WORKSPACE:?missing workspace path}"
: "${RUNNER_TEMP:?missing runner temporary directory}"
: "${CC:?missing selected C compiler}"
: "${CXX:?missing selected C++ compiler}"

fingerprint() {
    printf '%s\n' 'pangrova-linux-asan-stage-v1' \
        "run=$GITHUB_RUN_ID" "revision=$GITHUB_SHA" "workspace=$GITHUB_WORKSPACE" \
        "prefix=$root" "architecture=$(uname -m)"
    (. /etc/os-release; printf 'distribution=%s:%s\n' "$ID" "$VERSION_ID")
    "$CC" -dumpfullversion
    "$CXX" -dumpfullversion
    "$CXX" -dumpmachine
    cmake --version
    dpkg-query -W -f '${binary:Package}=${Version}\n' libc6 libstdc++6 libasan8 libssl3t64
}

verify_outputs() {
    local file
    for file in env.sh asan-env.sh velox/scripts/setup-ubuntu.sh velox/velox/core/PlanNode.h \
        velox/_build/asan-ci/CMakeCache.txt velox/_build/asan-ci/lib/libvelox.a \
        velox/_build/asan-ci/lib/libvelox.a.pangrova-provenance \
        native-deps-asan/lib/libprotobuf.a native-deps-asan/lib/libfolly.a \
        native-deps-asan/lib/libarrow_flight.so; do
        [[ -s "$root/$file" ]] || die "ASan stage lacks required output: $file"
    done
    [[ -d "$root/velox/.git" ]] || die "Velox source provenance checkout is absent"
    grep -Eq '^CMAKE_CXX_FLAGS:.*-fsanitize=address' "$root/velox/_build/asan-ci/CMakeCache.txt" \
        || die "ASan Velox archive is missing its instrumentation contract"
    [[ -s /usr/local/lib/cmake/AWSSDK/AWSSDKConfig.cmake ]] \
        || die "ASan stage lacks the Velox AWS SDK installation"
}

case "$operation" in
    pack)
        verify_outputs
        marker="$RUNNER_TEMP/asan-before-dependencies"
        [[ -f "$marker" ]] || die "Missing marker for the Velox installation"
        mkdir -p "$bundle"
        fingerprint > "$bundle/identity.txt"
        # Keep the Velox source identity, generated dependencies and installed
        # libraries. Downstream CMake does not compile the Velox object tree.
        tar -C "$root" --zstd -cf "$bundle/dependencies.tar.zst" \
            --exclude='*.o' --exclude='*.obj' --exclude='velox/archive-cache' \
            --exclude='velox/deps-download' --exclude='velox/_build/release' \
            env.sh asan-env.sh native-deps native-deps-asan velox
        # Velox's pinned Linux installer writes to /usr/local. Preserve its
        # actual install paths and symlinks without copying the runner image.
        # ctime includes installed headers that retain the source archive mtime.
        (
            cd /usr/local
            find bin include lib share -xdev \( -type f -o -type l \) -cnewer "$marker" -print0 \
                | tar --zstd -cf "$bundle/velox-install.tar.zst" --null --files-from=-
        )
        (cd "$bundle" && sha256sum dependencies.tar.zst velox-install.tar.zst > sha256sums)
        du -h "$bundle"/*.tar.zst
        ;;
    restore)
        # Compare the installed toolchain and runtime ABI. Image build numbers
        # can differ while the distribution, compiler and libraries still match.
        expected=$(mktemp)
        trap 'rm -f "$expected"' EXIT
        fingerprint > "$expected"
        if ! diff -u "$bundle/identity.txt" "$expected" >&2; then
            die "ASan stage revision, run, architecture or toolchain does not match"
        fi
        (cd "$bundle" && sha256sum dependencies.tar.zst velox-install.tar.zst | diff -u sha256sums -)
        for path in env.sh asan-env.sh native-deps native-deps-asan velox; do
            [[ ! -e "$root/$path" && ! -L "$root/$path" ]] \
                || die "ASan stage requires a fresh dependency prefix: $path"
        done
        mkdir -p "$root"
        tar -C "$root" --zstd --no-same-owner -xf "$bundle/dependencies.tar.zst"
        # Reinstall the pinned system packages, including their runtime libraries.
        # The compiled Velox install is restored from the same-run archive.
        (
            cd "$root/velox"
            sudo apt-get install -y libunwind-dev
            bash scripts/setup-ubuntu.sh install_velox_deps_from_apt install_faiss_deps
        )
        sudo tar -C /usr/local --zstd --no-same-owner --no-same-permissions \
            -xf "$bundle/velox-install.tar.zst"
        sudo ldconfig
        verify_outputs
        ;;
    *) die "Unknown operation: $operation" ;;
esac
