#!/usr/bin/env python3
"""Build a private LLVM 17 SDK with the upstream Mach-O sanitizer prefix fix.

Backports llvm/llvm-project c23135c5488fbfaf6439433a10b3ddef33ff112c.
The installed Homebrew SDK and its compiler-rt remain untouched. This relinks
the SDK's static components, so its exact SDK hashes are part of the evidence.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import urllib.request


VERSION = "17.0.6"
UPSTREAM_FIX = "c23135c5488fbfaf6439433a10b3ddef33ff112c"
PATCHED_SOURCE_SHA256 = "3994f5cb8c181fdc1c7dc783124697b0ac09bd0b79bacbccefab37b44355666a"
SOURCE_URL = ("https://raw.githubusercontent.com/llvm/llvm-project/llvmorg-17.0.6/"
              "llvm/lib/CodeGen/AsmPrinter/")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def capture(command, **kwargs):
    return subprocess.check_output(command, text=True, **kwargs).strip()


def run(command, root, name):
    (root / (name + "-command.json")).write_text(json.dumps([str(x) for x in command], indent=2))
    with (root / (name + ".log")).open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)


def fetch_sources(root, cache, manifest):
    source = root / "src"
    source.mkdir()
    for entry in manifest:
        name = entry["name"]
        require(Path(name).name == name, "Source manifest must contain flat filenames")
        if cache:
            data = (cache / name).read_bytes()
        else:
            with urllib.request.urlopen(SOURCE_URL + name, timeout=60) as response:
                data = response.read()
        require(hashlib.sha256(data).hexdigest() == entry["sha256"], "Source SHA mismatch: " + name)
        (source / name).write_bytes(data)
    return source


def clone_sdk(sdk, root):
    private = root / "toolchain"
    private.mkdir()
    for item in sdk.iterdir():
        if item.name not in {"bin", "lib"}:
            (private / item.name).symlink_to(item, target_is_directory=item.is_dir())
    for directory in ("bin", "lib"):
        (private / directory).mkdir()
        for item in (sdk / directory).iterdir():
            target = private / directory / item.name
            if directory == "lib" and item.name in {"libLLVM.dylib", "libLLVMAsmPrinter.a"}:
                continue
            if directory == "lib" and item.name == "libLLVM-17.dylib":
                target.symlink_to("libLLVM.dylib")
            elif directory == "lib" and item.name == "libclang-cpp.dylib":
                shutil.copy2(item, target)
            elif directory == "bin" and item.name in {"clang", "clang++"}:
                target.symlink_to("clang-17")
            elif directory == "bin" and item.name in {"clang-17", "llc", "llvm-as", "llvm-dis"}:
                shutil.copy2(item, target)
            else:
                target.symlink_to(item, target_is_directory=item.is_dir())
    return private


def system_dependencies(sdk):
    """Use the installed dylib's actual dependencies, including relocated SDKs."""
    lines = capture(["otool", "-L", sdk / "lib/libLLVM.dylib"]).splitlines()
    result = []
    for line in lines[2:]:  # The first dependency entry is this dylib's install ID.
        dependency = line.strip().split(" (compatibility version", 1)[0]
        if dependency.startswith("/usr/lib/lib") and dependency.endswith(".dylib"):
            result.append("-l" + Path(dependency).name[3:-6])
        else:
            require(Path(dependency).is_file(), "Unresolved LLVM dependency: " + dependency)
            result.append(dependency)
    return result


def rebuild_llvm(sdk, root, private, source):
    config = sdk / "bin/llvm-config"
    flags = shlex.split(capture([config, "--cxxflags"]))
    obj = root / "AsmPrinter.cpp.o"
    run([sdk / "bin/clang++", '-DPACKAGE_NAME="LLVM"', '-DPACKAGE_VERSION="17.0.6"',
         '-DPACKAGE_VENDOR="Homebrew"', *flags, "-O2", "-DNDEBUG", "-fPIC", "-c",
         source / "AsmPrinter.cpp", "-o", obj], root, "compile")
    archive = private / "lib/libLLVMAsmPrinter.a"
    shutil.copy2(sdk / "lib/libLLVMAsmPrinter.a", archive)
    archive.chmod(0o644)
    run([sdk / "bin/llvm-ar", "r", archive, obj], root, "archive")
    run([sdk / "bin/llvm-ranlib", archive], root, "ranlib")
    # llvm/tools/llvm-shlib/CMakeLists.txt excludes TableGen from LLVM_DYLIB_COMPONENTS=all.
    # Including it registers a second global command-line option named "o".
    names = shlex.split(capture([config, "--link-static", "--libnames", "all"]))
    names = [name for name in names if name != "libLLVMTableGen.a"]
    require(names.count("libLLVMAsmPrinter.a") == 1, "SDK lacks a unique AsmPrinter archive")
    inputs = [sdk / "lib" / name for name in names]
    sdk_evidence = [{"name": p.name, "sha256": sha256(p)} for p in inputs]
    (root / "sdk-archive-inputs.json").write_text(json.dumps(sdk_evidence, indent=2))
    inputs = [archive if p.name == archive.name else p for p in inputs]
    run([sdk / "bin/clang++", "-dynamiclib", "-Wl,-install_name,@rpath/libLLVM.dylib",
         "-Wl,-compatibility_version,1", "-Wl,-current_version,17.0.6", "-Wl,-all_load",
         *inputs, "-Wl,-noall_load", *system_dependencies(sdk), "-o",
         private / "lib/libLLVM.dylib"], root, "link")


def calibrate(private, root):
    """Prove prefix association, real SDK loading and both sanitizer rejection paths."""
    environment = dict(os.environ, DYLD_PRINT_LIBRARIES="1")
    result = subprocess.run([private / "bin/llc", "--version"], env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True)
    (root / "compiler-loader.log").write_text(result.stdout)
    require(str((private / "lib/libLLVM.dylib").resolve()) in result.stdout,
            "Private llc did not load the rebuilt LLVM dylib")
    ir = root / "func-sanitizer.ll"
    ir.write_text('target triple = "arm64-apple-darwin"\n'
                  'define void @valid() nounwind !func_sanitize !0 { ret void }\n'
                  '!0 = !{i32 -1056584962, i32 42}\n')
    assembly = root / "func-sanitizer.s"
    run([private / "bin/llc", ir, "-o", assembly], root, "assembly-canary")
    require(".alt_entry\t_valid" in assembly.read_text(), "Function sanitizer metadata lacks .alt_entry")
    cases = {
        "valid": ('void f(){} int main(){auto p=&f; p();}\n', None),
        "invalid-function": ('void f(){} int main(){auto p=reinterpret_cast<void(*)(int)>(&f); p(1);}\n',
                             "incorrect function type"),
        "invalid-memory": ('#include <cstdlib>\nint main(int argc,char**){'
                           'auto p=(volatile unsigned char*)std::malloc(1); p[argc]=7;'
                           'std::free((void*)p);}\n', "AddressSanitizer: heap-buffer-overflow"),
    }
    evidence = []
    for name, (code, diagnostic) in cases.items():
        source, binary = root / (name + ".cpp"), root / name
        source.write_text(code)
        run([private / "bin/clang++", "-O1", "-g", "-fsanitize=address,function",
             "-fno-sanitize-recover=all", source, "-o", binary], root, name + "-build")
        env = dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                   ASAN_SYMBOLIZER_PATH=str(private / "bin/llvm-symbolizer"))
        result = subprocess.run([binary], env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        (root / (name + "-run.log")).write_text(result.stdout)
        if diagnostic:
            require(result.returncode != 0 and diagnostic in result.stdout,
                    "Sanitizer failed to reject " + name)
        else:
            require(result.returncode == 0 and "runtime error:" not in result.stdout
                    and "AddressSanitizer:" not in result.stdout, "Valid call failed calibration")
        evidence.append({"case": name, "child_exit": result.returncode, "diagnostic": diagnostic})
    (root / "canary-evidence.json").write_text(json.dumps(evidence, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llvm-prefix", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="New private directory; never overwritten")
    parser.add_argument("--source-cache", type=Path, help="Read verified original source files here, without network")
    args = parser.parse_args()
    require(sys.version_info >= (3, 11), "Python 3.11+ is required")
    require(sys.platform == "darwin" and platform.machine() == "arm64", "Requires macOS arm64")
    sdk = args.llvm_prefix.resolve(strict=True)
    require(capture([sdk / "bin/llvm-config", "--version"]) == VERSION, "Requires LLVM 17.0.6 SDK")
    require(capture([sdk / "bin/llc", "--version"]).startswith("Homebrew LLVM version " + VERSION),
            "Requires the Homebrew LLVM 17.0.6 SDK configuration")
    require(capture([sdk / "bin/llvm-config", "--build-mode"]) == "Release", "Requires the Release LLVM SDK")
    require(args.output.is_absolute() and not args.output.exists(), "Output must be a new absolute directory")
    root = args.output
    root.mkdir(parents=True)
    root = root.resolve()
    patches = Path(__file__).resolve().parent.parent / "patches"
    manifest = json.loads((patches / "llvm-17-function-prefix-sources.json").read_text())
    source = fetch_sources(root, args.source_cache, manifest)
    patch = patches / "llvm-17-darwin-function-prefix.patch"
    with patch.open("rb") as stream, (root / "patch.log").open("wb") as log:
        subprocess.run(["patch", "--batch", "--forward", "-p1", "-d", source],
                       stdin=stream, stdout=log, stderr=subprocess.STDOUT, check=True)
    require(sha256(source / "AsmPrinter.cpp") == PATCHED_SOURCE_SHA256, "Patched source SHA mismatch")
    private = clone_sdk(sdk, root)
    rebuild_llvm(sdk, root, private, source)
    calibrate(private, root)
    sdk_inputs = ["bin/clang-17", "bin/llc", "bin/llvm-config", "lib/libclang-cpp.dylib",
                  "include/llvm/CodeGen/AsmPrinter.h", "include/llvm/Config/llvm-config.h",
                  "include/llvm/Config/abi-breaking.h", "include/llvm/Support/Compiler.h"]
    result = {"upstream_fix": UPSTREAM_FIX, "sdk": str(sdk), "sdk_llvm_sha256": sha256(sdk / "lib/libLLVM.dylib"),
              "sdk_inputs": [{"name": name, "sha256": sha256(sdk / name)} for name in sdk_inputs],
              "patch_sha256": sha256(patch), "patched_source_sha256": PATCHED_SOURCE_SHA256,
              "llvm_sha256": sha256(private / "lib/libLLVM.dylib"), "source_inputs": manifest}
    (root / "build-evidence.json").write_text(json.dumps(result, indent=2))
    print("Private compiler: " + str(private / "bin/clang++"))


if __name__ == "__main__":
    main()
