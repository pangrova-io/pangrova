"""Measure actual changed executable lines, including staged and untracked Java."""
import argparse
import re
import subprocess
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

from classfile_evidence import has_no_executable_code


def git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments])


def changed_lines(root, base):
    # A three-dot/HEAD diff silently misses local work. Compare directly with the worktree.
    git(root, "rev-parse", "--verify", base + "^{commit}")
    tracked = git(root, "diff", "--name-only", "-z", "--diff-filter=ACMR", base, "--", "*.java")
    untracked = git(root, "ls-files", "--others", "--exclude-standard", "-z", "--", "*.java")
    new_files = {item.decode() for item in untracked.split(b"\0") if item}
    files = {item.decode() for item in tracked.split(b"\0") if item} | new_files
    result = {}
    for name in sorted(files):
        if "/src/main/java/" not in name or "/target/" in name:
            continue
        path = root / name
        if not path.is_file():
            continue
        if name in new_files:
            lines = set(range(1, len(path.read_bytes().splitlines()) + 1))
        else:
            patch = git(root, "diff", "--no-ext-diff", "--unified=0", base, "--", name).decode()
            lines = set()
            for match in re.finditer(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", patch, re.MULTILINE):
                start = int(match[1])
                count = int(match[2]) if match[2] is not None else 1
                lines.update(range(start, start + count))
        if lines:
            result[name] = lines
    return result


def report_lines(root, name):
    module, source = name.split("/src/main/java/", 1)
    report = root / module / "target/site/jacoco/jacoco.xml"
    if not report.is_file():
        if has_no_executable_code(root, name):
            return {}
        raise ValueError(f"Missing report: {report}. Run verify -Pcoverage.")
    if report.stat().st_mtime < (root / name).stat().st_mtime:
        raise ValueError(f"Stale coverage report for {name}; rerun verify -Pcoverage.")
    package, filename = source.rsplit("/", 1)
    tree = ET.parse(report)
    for node in tree.getroot().findall("package"):
        if node.get("name") != package:
            continue
        for file in node.findall("sourcefile"):
            if file.get("name") == filename:
                return {int(line.get("nr")): int(line.get("ci")) > 0 for line in file.findall("line")}
    if has_no_executable_code(root, name):
        return {}
    raise ValueError(f"No source coverage for {name}; absence is not a passing measurement.")


def measure(root, changes):
    covered = total = 0
    missing = []
    results = []
    for name, added in changes.items():
        try:
            executable = report_lines(root, name)
        except (ValueError, ET.ParseError, OSError) as error:
            missing.append(str(error))
            continue
        measured = added.intersection(executable)
        hit = sum(executable[line] for line in measured)
        total += len(measured)
        covered += hit
        results.append((name, hit, len(measured)))
    return covered, total, results, missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/main")
    parser.add_argument("--threshold", type=float, default=60)
    args = parser.parse_args()
    if not 0 <= args.threshold <= 100:
        parser.error("threshold must be between 0 and 100")
    root = Path(git(Path(__file__).resolve().parent, "rev-parse", "--show-toplevel").decode().strip())
    try:
        changes = changed_lines(root, args.base_ref)
        covered, total, results, missing = measure(root, changes)
    except (subprocess.CalledProcessError, OSError, UnicodeError) as error:
        print(f"FAIL: cannot measure the actual diff: {error}", file=sys.stderr)
        return 1
    for name, hit, count in results:
        print(f"{name}: {hit}/{count} changed executable lines")
    if missing:
        for error in missing:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1
    if not changes:
        print("No production Java changes; coverage measurement is not applicable.")
        return 0
    if total == 0:
        print("Changed files have reports but no changed executable lines.")
        return 0
    percent = 100 * covered / total
    print(f"Actual diff coverage: {percent:.1f}% ({covered}/{total}), threshold {args.threshold:g}%")
    return 0 if percent >= args.threshold else 1


if __name__ == "__main__":
    sys.exit(main())
