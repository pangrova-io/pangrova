#!/usr/bin/env python3
"""Bind a Lance archive to the exact owned Rust, header and protocol inputs."""

import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def inputs(root):
    native = root / "pangrova-worker-native"
    crate = native / "lance-ffi"
    files = [crate / name for name in ("Cargo.toml", "Cargo.lock", "build.rs", "rust-toolchain.toml",
                                      ".cargo/config.toml")]
    files += [native / "bin/prepare-lance-sdk.sh", native / "patches/lance-writer-drain.patch"]
    for directory in (crate / "src", crate / "include",
                      root / "pangrova-protocol/src/main/proto",
                      root / "pangrova-protocol/src/main/proto-imports"):
        if not directory.is_dir():
            raise ValueError(f"Missing Lance build input directory: {directory}")
        files.extend(path for path in directory.rglob("*") if path.is_file())
    return {str(path.relative_to(root)): digest(path) for path in sorted(files)}


def seal(root, archive, header, target, captured):
    before = json.loads(captured.read_text())
    if before != inputs(root):
        raise ValueError("Lance inputs changed during compilation; rebuild before linking")
    expected_header = root / "pangrova-worker-native/lance-ffi/include/lance_c_ffi.h"
    if digest(header) != digest(expected_header):
        raise ValueError("Lance installed header differs from its source")
    receipt = {"version": 1, "target": target, "inputs": before,
               "archive_sha256": digest(archive), "header_sha256": digest(header)}
    path = Path(str(archive) + ".provenance.json")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def verify(root, archive, header, target):
    path = Path(str(archive) + ".provenance.json")
    receipt = json.loads(path.read_text())
    expected = {"version": 1, "target": target, "inputs": inputs(root),
                "archive_sha256": digest(archive), "header_sha256": digest(header)}
    if receipt != expected:
        raise ValueError("Lance archive/header/source provenance differs; rebuild with build-lance-ffi.sh")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "seal", "verify"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--capture", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--header", type=Path)
    parser.add_argument("--target")
    args = parser.parse_args()
    try:
        if args.mode == "capture":
            args.capture.write_text(json.dumps(inputs(args.root), sort_keys=True))
        elif args.mode == "seal":
            seal(args.root, args.archive, args.header, args.target, args.capture)
        else:
            verify(args.root, args.archive, args.header, args.target)
    except (OSError, ValueError) as failure:
        parser.exit(1, f"Lance C FFI provenance rejected: {failure}\n")


if __name__ == "__main__":
    main()
