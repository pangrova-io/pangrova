#!/usr/bin/env python3
"""Patch Arrow Flight types.h to fix incomplete type issue with Apple libc++.

Problem: FlightInfo(Data data) is inline-defined before FlightEndpoint is fully
defined. Apple libc++'s std::vector move constructor requires complete types.

Fix: Change the inline constructor to a declaration, add out-of-line definition
after FlightEndpoint is fully defined (but still inside namespace flight).
"""
import argparse
import os
import sys
from pathlib import Path

HEADER_RELATIVE_PATH = Path("include/arrow/flight/types.h")
OLD_LINE = "  explicit FlightInfo(Data data) : data_(std::move(data)), reconstructed_schema_(false) {}\n"
NEW_LINE = "  explicit FlightInfo(Data data);\n"
DEFINITION = [
    '\n',
    '// Out-of-line: deferred past FlightEndpoint for Apple libc++ vector move.\n',
    'inline FlightInfo::FlightInfo(Data data)\n',
    '    : data_(std::move(data)), reconstructed_schema_(false) {}\n',
    '\n',
]
NAMESPACE_MARKER = '}  // namespace flight\n'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--header", type=Path, help="path to Arrow Flight types.h")
    target.add_argument("--deps-prefix", type=Path, help="native dependency installation prefix")
    return parser.parse_args()


def resolve_header(args: argparse.Namespace) -> Path:
    if args.header:
        return args.header.expanduser()
    if args.deps_prefix:
        return args.deps_prefix.expanduser() / HEADER_RELATIVE_PATH

    deps_prefix = os.environ.get("PANGROVA_DEPS_PREFIX")
    if deps_prefix:
        return Path(deps_prefix).expanduser() / HEADER_RELATIVE_PATH

    pangrova_home = Path(os.environ.get("PANGROVA_HOME", Path.home() / ".pangrova")).expanduser()
    return pangrova_home / "native-deps" / HEADER_RELATIVE_PATH


def patch_header(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"Arrow Flight header not found: {path}. Set PANGROVA_DEPS_PREFIX or pass --header/--deps-prefix."
        )

    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    definition_text = "".join(DEFINITION[1:4])
    content = "".join(lines)
    if NEW_LINE in lines and definition_text in content:
        print(f"Patch already applied: {path}")
        return

    try:
        constructor_index = lines.index(OLD_LINE)
    except ValueError as error:
        raise RuntimeError("Could not find inline FlightInfo constructor to replace") from error

    lines[constructor_index] = NEW_LINE
    print(f"Step 1: Replaced inline constructor at line {constructor_index + 1}")

    try:
        namespace_index = len(lines) - 1 - lines[::-1].index(NAMESPACE_MARKER)
    except ValueError as error:
        raise RuntimeError("Could not find Arrow Flight namespace marker") from error

    print(f"Step 2: Inserting definition before line {namespace_index + 1}")
    lines[namespace_index:namespace_index] = DEFINITION
    path.write_text("".join(lines), encoding="utf-8")
    print(f"Patch applied successfully: {path}")


def main() -> int:
    args = parse_args()
    try:
        patch_header(resolve_header(args))
    except (FileNotFoundError, RuntimeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
