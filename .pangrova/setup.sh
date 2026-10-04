#!/usr/bin/env bash
# Verify the canonical repository instructions without creating tool adapters.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ $# -gt 1 || ( $# -eq 1 && "${1}" != "--check" ) ]]; then
  printf 'Usage: %s [--check]\n' "$0" >&2
  exit 2
fi

[[ -f "$PROJECT_ROOT/AGENTS.md" && ! -L "$PROJECT_ROOT/AGENTS.md" ]] || {
  printf 'ERROR AGENTS.md must be a regular repository instruction file\n' >&2
  exit 1
}
[[ -d "$PROJECT_ROOT/.pangrova/rules" && ! -L "$PROJECT_ROOT/.pangrova/rules" ]] || {
  printf 'ERROR canonical rules directory must exist in .pangrova\n' >&2
  exit 1
}
for rule in architecture code-standards development testing build-pipeline ci-gates workflow; do
  [[ -f "$PROJECT_ROOT/.pangrova/rules/$rule.md" ]] || {
    printf 'ERROR missing canonical rule: %s.md\n' "$rule" >&2
    exit 1
  }
done
printf 'Canonical instructions are valid: AGENTS.md + .pangrova/rules/\n'
