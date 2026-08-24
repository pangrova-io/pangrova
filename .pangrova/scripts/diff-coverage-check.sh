#!/usr/bin/env bash
# Diff-based JaCoCo coverage gate.
# Enforces minimum line coverage on new/modified Java source files.
# Usage: diff-coverage-check.sh [--threshold 60] [--base-ref origin/main]
#
# Exit codes:
#   0 — coverage threshold met (or no measurable changes)
#   1 — coverage below threshold

set -euo pipefail

THRESHOLD=60
BASE_REF="origin/main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threshold) THRESHOLD="$2"; shift 2 ;;
    --base-ref)  BASE_REF="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "=== Diff Coverage Check ==="
echo "Threshold: ${THRESHOLD}% line coverage on new/modified code"
echo "Base ref:  ${BASE_REF}"
echo ""

# Collect changed Java source files (exclude test files)
CHANGED_FILES=$(git diff --name-only --diff-filter=ACMR "${BASE_REF}"...HEAD -- '*.java' \
  | grep -v '/src/test/' \
  | grep -v '/target/' \
  || true)

if [[ -z "${CHANGED_FILES}" ]]; then
  echo "No production Java files changed. Skipping coverage check."
  exit 0
fi

echo "Changed production files:"
echo "${CHANGED_FILES}" | sed 's/^/  /'
echo ""

# Locate JaCoCo XML reports across all modules
REPORTS=$(find . -path '*/target/site/jacoco/jacoco.xml' -type f 2>/dev/null || true)

if [[ -z "${REPORTS}" ]]; then
  echo "ERROR: No JaCoCo reports found. Run 'mvn verify -Pcoverage' first."
  exit 1
fi

# Parse coverage from JaCoCo XML reports for changed files.
python3 - "${THRESHOLD}" "${CHANGED_FILES}" ${REPORTS} << 'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET

threshold = int(sys.argv[1])
changed_files_str = sys.argv[2]
report_paths = sys.argv[3:]

changed_files = [f.strip() for f in changed_files_str.strip().split('\n') if f.strip()]


def source_file_to_jacoco_key(filepath):
    """Convert e.g. pangrova-common/src/main/java/org/pangrova/common/Foo.java -> org/pangrova/common/Foo"""
    parts = filepath.split('/src/main/java/')
    if len(parts) != 2:
        return None
    return parts[1].replace('.java', '')


changed_keys = {}
for f in changed_files:
    key = source_file_to_jacoco_key(f)
    if key:
        changed_keys[key] = f

if not changed_keys:
    print("No measurable production files in diff. Skipping.")
    sys.exit(0)

total_lines = 0
covered_lines = 0
file_results = []

for report_path in report_paths:
    try:
        tree = ET.parse(report_path)
        root = tree.getroot()
    except ET.ParseError:
        continue

    for package in root.findall('.//package'):
        pkg_name = package.get('name', '')
        for sourcefile in package.findall('sourcefile'):
            source_name = sourcefile.get('name', '')
            class_key = pkg_name + '/' + source_name.replace('.java', '')

            if class_key in changed_keys:
                missed = 0
                hit = 0
                for counter in sourcefile.findall('counter'):
                    if counter.get('type') == 'LINE':
                        missed = int(counter.get('missed', '0'))
                        hit = int(counter.get('covered', '0'))
                        break

                file_total = missed + hit
                if file_total > 0:
                    total_lines += file_total
                    covered_lines += hit
                    pct = round(100.0 * hit / file_total, 1)
                    file_results.append((changed_keys[class_key], hit, file_total, pct))

if total_lines == 0:
    print("No measurable coverage data for changed files. Skipping.")
    sys.exit(0)

overall_pct = round(100.0 * covered_lines / total_lines, 1)

# Print per-file breakdown (sorted worst-first)
print("Per-file coverage:")
for filepath, hit, total, pct in sorted(file_results, key=lambda x: x[3]):
    status = "PASS" if pct >= threshold else "FAIL"
    print(f"  {status} {pct:5.1f}% ({hit}/{total} lines) {filepath}")

print("")
print("=== Summary ===")
print(f"Overall diff coverage: {overall_pct}% ({covered_lines}/{total_lines} lines)")
print(f"Threshold:             {threshold}%")
print("")

if overall_pct < threshold:
    print(f"FAILED: Diff coverage {overall_pct}% is below the {threshold}% threshold.")
    print("Please add tests for the new/modified code.")
    sys.exit(1)

print(f"PASSED: Diff coverage meets the {threshold}% threshold.")
sys.exit(0)
PYTHON_SCRIPT
