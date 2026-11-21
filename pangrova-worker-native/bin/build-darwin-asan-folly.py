#!/usr/bin/env python3
"""Build the Darwin ASan Folly dependency with pinned integer-source corrections."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import shlex
import shutil
import subprocess

PATCHES = Path(__file__).resolve().parents[1] / "patches"


def patched_copy(source, target, patch, entries):
    for relative, digests in entries.items():
        if hashlib.sha256((source / relative).read_bytes()).hexdigest() != digests["upstream"]:
            raise ValueError(f"Upstream source digest mismatch: {source / relative}")
    shutil.copytree(source, target, ignore=shutil.ignore_patterns(".git", "_build*"))
    subprocess.run(["patch", "--batch", "--forward", "-p1", "-i", PATCHES / patch],
                   cwd=target, check=True)
    for relative, digests in entries.items():
        if hashlib.sha256((target / relative).read_bytes()).hexdigest() != digests["patched"]:
            raise ValueError(f"Patched source digest mismatch: {target / relative}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--folly-source", type=Path, required=True)
    parser.add_argument("--compiler-prefix", type=Path, required=True,
                        help="Qualified private LLVM 17.0.6 function-prefix toolchain")
    parser.add_argument("--prefix-path", required=True, help="Dependency prefixes for CMAKE_PREFIX_PATH")
    parser.add_argument("--output", type=Path, required=True, help="A new directory")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--openssl-root", type=Path, help="OpenSSL installation used by Velox")
    args = parser.parse_args()
    if platform.system() != "Darwin" or platform.machine() != "arm64" or args.jobs < 1:
        parser.error("Requires Darwin arm64 and a positive job count")
    root = args.output.resolve()
    compiler = args.compiler_prefix.resolve()
    folly = args.folly_source.resolve()
    libcxx = (compiler / "include/c++/v1").resolve()
    if any(root.is_relative_to(source) for source in (folly, compiler, libcxx)):
        parser.error("Use an output directory outside the source and compiler prefixes")
    version = subprocess.check_output([compiler / "bin/clang++", "--version"], text=True)
    if "clang version 17.0.6 " not in version and "clang version 17.0.6\n" not in version:
        parser.error("A qualified LLVM 17.0.6 compiler is required")
    assembly = subprocess.check_output(
        [compiler / "bin/clang++", "-x", "c++", "-fsanitize=function", "-S", "-o", "-", "-"],
        input='extern "C" void pangrova_sanitizer_probe() {}\n', text=True)
    if ".alt_entry\t_pangrova_sanitizer_probe" not in assembly:
        parser.error("Compiler lacks the Darwin function sanitizer metadata fix")
    root.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((PATCHES / "darwin-integer-semantics-sources.json").read_text())
    folly_patch = "folly-2026.01.05-integer-semantics.patch"
    libcxx_patch = "libcxx-17.0.6-seed-seq-word.patch"
    patched_copy(folly, root / "folly", folly_patch, manifest[folly_patch])
    headers = root / "libcxx"
    patched_copy(libcxx, headers, libcxx_patch, manifest[libcxx_patch])
    flags = "-mcpu=apple-m1+crc"
    configure = ["cmake", "-S", root / "folly", "-B", root / "build", "-G", "Ninja",
                 "-DCMAKE_BUILD_TYPE=Release", "-DBUILD_SHARED_LIBS=OFF",
                 "-DBUILD_TESTS=OFF", "-DFOLLY_LIBRARY_SANITIZE_ADDRESS=ON",
                 f"-DCMAKE_C_COMPILER={compiler}/bin/clang",
                 f"-DCMAKE_CXX_COMPILER={compiler}/bin/clang++", f"-DCMAKE_C_FLAGS={flags}",
                 f"-DCMAKE_CXX_FLAGS={flags} -nostdinc++ -isystem {shlex.quote(str(headers))}",
                 f"-DCMAKE_PREFIX_PATH={args.prefix_path}",
                 f"-DCMAKE_PROJECT_folly_INCLUDE={PATCHES.parent}/cmake/AsanFollyCompat.cmake",
                 f"-DCMAKE_INSTALL_PREFIX={root}/install"]
    if args.openssl_root:
        configure.append(f"-DOPENSSL_ROOT_DIR={args.openssl_root.resolve()}")
    (root / "configure-command.json").write_text(json.dumps(list(map(str, configure)), indent=2))
    subprocess.run(configure, check=True)
    subprocess.run(["cmake", "--build", root / "build", "--target", "install",
                    "--parallel", str(args.jobs)], check=True)
    archive = root / "install/lib/libfolly.a"
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    print(f"Installed {archive} (SHA-256 {digest.hexdigest()})")


if __name__ == "__main__":
    main()
