#!/usr/bin/env python3
"""Compare record-credit exhaustion using two ShuffleBufferStore implementations and a common macOS SDK."""
import argparse
import csv
import json
from pathlib import Path
import shlex
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-native", type=Path, required=True)
    parser.add_argument("--candidate-native", type=Path, required=True)
    parser.add_argument("--baseline-build", type=Path, required=True,
                        help="Makefile-based production build with compile_commands.json and link.txt")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline = args.baseline_native.resolve(strict=True)
    candidate = args.candidate_native.resolve(strict=True)
    build = args.baseline_build.resolve(strict=True)
    commands = json.loads((build / "compile_commands.json").read_text())
    entry = next(e for e in commands if e["file"].endswith("/ShuffleBufferStore.cpp"))
    compile_args = shlex.split(entry["command"])
    common_args = compile_args[:compile_args.index("-o")]
    link_command = shlex.split((build / "CMakeFiles/pangrova_native.dir/link.txt").read_text())
    libraries = [Path(arg) for arg in link_command if arg.startswith("/")]
    arrow = next(lib for lib in libraries if lib.name.startswith("libarrow.") and lib.suffix == ".dylib")
    folly = next(lib for lib in libraries if lib.name == "libfolly.a")
    sdk = folly.parent
    libs = [arrow, folly, sdk / "libglog.dylib", sdk / "libdouble-conversion.a", sdk / "libfmt.a"]
    cpp = Path(common_args[0]).parent.parent / "lib/c++"
    cpp_flags = ["-L" + str(cpp), "-Wl,-rpath," + str(cpp)] if cpp.is_dir() else []
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    driver = Path(__file__).with_name("ShuffleRetentionComparison.cpp")
    with tempfile.TemporaryDirectory(prefix="shuffle-retention-") as tmp:
        work = Path(tmp)
        for name, source, scoped in (("baseline", baseline, False), ("candidate", candidate, True)):
            # Reuse the exact SDK/include options while replacing the implementation and its headers together.
            options = [arg.replace(str(baseline / "src"), str(source / "src")) for arg in common_args]
            options.append("-DPER_HANDLE_RETENTION=" + str(int(scoped)))
            objects = []
            for translation_unit in [source / "src/engine/shuffle/ShuffleBufferStore.cpp", driver]:
                obj = work / (name + translation_unit.stem + ".o")
                subprocess.run([*options, "-c", translation_unit, "-o", obj], check=True)
                objects.append(obj)
            executable = work / name
            subprocess.run([options[0], *objects, "-Wl,-dead_strip", *cpp_flags,
                            "-Wl,-rpath," + str(arrow.parent), "-Wl,-rpath," + str(sdk),
                            *libs, "-o", executable], check=True)
            output = out / (name + "-retention.csv")
            with output.open("x") as stream:
                subprocess.run([executable], stdout=stream, check=True)
            with output.open() as stream:
                rows = list(csv.DictReader(stream))
            expected = "OK" if scoped else "CAPACITY_ERROR"
            if len(rows) != 12 or any(row["store_status"] != expected for row in rows):
                raise ValueError(f"Unexpected producer outcome in {output}")


if __name__ == "__main__":
    main()
