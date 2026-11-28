#!/usr/bin/env python3
"""Keep Arrow's existing public exports and localize static dependency symbols.

An exact list from the first link avoids promoting deliberately hidden C++
helpers (as a namespace wildcard export list would). The public policy matches
Arrow's upstream symbols.map; verification also checks that no public symbol
was lost in the second link.
"""

import argparse
import json
import re
import subprocess
from pathlib import Path


STATUS_MESSAGE_STORAGE = (
    "__ZZNK5arrow6Status7messageEvE10no_message",
    "__ZGVZNK5arrow6Status7messageEvE10no_message",
)


def is_public(symbol: str) -> bool:
    if symbol in STATUS_MESSAGE_STORAGE:
        return False
    name = symbol.removeprefix("_")  # Mach-O adds one leading underscore.
    if name.startswith(("arrow_", "Arrow", "descriptor_table_Flight")):
        return True
    return name.startswith("_Z") and bool(
        re.search(r"(?<![0-9])(?:5arrow|14arrow_vendored|13opentelemetry)", name)
    )


def exported_symbols(library: Path) -> set[str]:
    output = subprocess.check_output(["nm", "-gU", str(library)], text=True)
    symbols = {line.split()[-1] for line in output.splitlines() if line.strip()}
    if not symbols or not any(is_public(symbol) for symbol in symbols):
        raise RuntimeError(f"No public Arrow exports in {library}")
    return symbols


def unresolved_static_imports(output: str) -> list[str]:
    return sorted(
        line.strip() for line in output.splitlines()
        if "(undefined)" in line and (
            "dynamically looked up" in line
            or re.search(r"6google8protobuf|4absl|4grpc|9grpc_core|grpc_event_engine|3re2", line)
            or re.search(r"external _(grpc_|gpr_|upb_)", line)
        )
    )


def private_imports(library: Path) -> list[str]:
    return unresolved_static_imports(subprocess.check_output(["nm", "-m", str(library)], text=True))


def prepare(build_dir: Path, seed: Path) -> None:
    expected = {}
    private = set()
    for name in ("libarrow.dylib", "libarrow_flight.dylib"):
        symbols = exported_symbols(build_dir / "release" / name)
        expected[name] = sorted(symbol for symbol in symbols if is_public(symbol))
        private.update(symbol for symbol in symbols if not is_public(symbol))
    (build_dir / "arrow-private-symbols.list").write_text(
        seed.read_text() + "\n" + "\n".join(sorted(private)) + "\n"
    )
    (build_dir / "arrow-public-symbols.json").write_text(json.dumps(expected, sort_keys=True))
    print(f"Localizing {len(private)} private exports and retaining the public Arrow ABI")


def verify(build_dir: Path) -> None:
    expected = json.loads((build_dir / "arrow-public-symbols.json").read_text())
    for name, public in expected.items():
        library = build_dir / "release" / name
        actual = exported_symbols(library)
        if actual != set(public):
            extra = sorted(actual - set(public))[:10]
            missing = sorted(set(public) - actual)[:10]
            raise RuntimeError(f"Arrow export mismatch in {name}: extra={extra}, missing={missing}")
        unresolved = private_imports(library)
        if unresolved:
            raise RuntimeError(f"Unresolved static dependency imports in {name}: {unresolved[:10]}")
        fixups = subprocess.check_output(["xcrun", "dyld_info", "-fixups", str(library)], text=True)
        for line in fixups.splitlines():
            if "weak-bind" in line and any(symbol in line for symbol in STATUS_MESSAGE_STORAGE):
                raise RuntimeError(f"Arrow status storage can coalesce across libraries in {name}: {line.strip()}")
        print(f"Verified {name}: {len(public)} public exports and private status storage")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--seed", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        verify(args.build_dir)
    elif args.seed:
        prepare(args.build_dir, args.seed)
    else:
        parser.error("--seed is required for the initial link")


if __name__ == "__main__":
    main()
