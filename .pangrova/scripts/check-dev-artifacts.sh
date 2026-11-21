#!/usr/bin/env bash
# Reject tracked or staged development artifacts and a recreated docs/internal.
# Local leftovers produce a warning; --strict upgrades them to a failure.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  printf 'check-dev-artifacts: not inside a git repository\n' >&2
  exit 1
}
cd "$ROOT"

STRICT=0
QUIET=0

usage() {
  printf 'Usage: %s [--strict] [--quiet]\n' "$0" >&2
}

for arg in "$@"; do
  case "$arg" in
    --strict) STRICT=1 ;;
    --quiet) QUIET=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

fail=0
TEMP_RE='(^|/)(javac\..*\.args|\.jqwik-database|dependency-reduced-pom\.xml)$|(^|/)__pycache__/|\.pyc$'

if [ -e docs/internal ] || [ -L docs/internal ]; then
  printf 'ERROR: retired documentation directory exists: docs/internal\n' >&2
  fail=1
fi

tracked_or_staged="$(
  {
    git ls-files
    git diff --cached --name-only
  } | sort -u
)"

violations="$(printf '%s\n' "$tracked_or_staged" | grep -E "$TEMP_RE" || true)"
if [ -n "$violations" ]; then
  printf 'ERROR: tracked or staged development artifacts:\n%s\n' "$violations" >&2
  fail=1
fi

untracked="$(
  find . -type d \( -name .git -o -name node_modules -o -name target \) -prune -o \
    \( -name 'javac.*.args' -o -name '.jqwik-database' -o \
       -name 'dependency-reduced-pom.xml' -o -name '*.pyc' -o \
       -type d -name '__pycache__' \) -print 2>/dev/null || true
)"

if [ -n "$untracked" ]; then
  if [ "$STRICT" -eq 1 ]; then
    printf 'ERROR: untracked development artifacts present:\n%s\n' "$untracked" >&2
    fail=1
  else
    printf 'WARNING: development artifacts present; run .pangrova/scripts/clean-dev-artifacts.sh\n%s\n' "$untracked" >&2
  fi
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

if [ "$QUIET" -eq 0 ]; then
  printf 'Dev artifact hygiene: OK\n'
fi
