#!/usr/bin/env bash
# Remove development-only artifacts that must not accumulate in the worktree.
# Default scope is ephemeral caches; --all also removes build outputs.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  printf 'clean-dev-artifacts: not inside a git repository\n' >&2
  exit 1
}
cd "$ROOT"

DRY_RUN=0
ALL=0
QUIET=0

usage() {
  printf 'Usage: %s [--all] [--dry-run] [--quiet]\n' "$0" >&2
}

for arg in "$@"; do
  case "$arg" in
    --all) ALL=1 ;;
    --dry-run) DRY_RUN=1 ;;
    --quiet) QUIET=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

say() {
  if [ "$QUIET" -eq 0 ]; then
    printf '%s\n' "$*"
  fi
}

removed=0

is_protected() {
  case "$1" in
    ./.git|./.git/*|./website/node_modules|./website/node_modules/*)
      return 0 ;;
    ./data/worker-admission|./data/worker-admission/*)
      return 0 ;;
    ./.env|./.env.*|./.secrets|./.secrets/*|./secrets|./secrets/*)
      return 0 ;;
    ./.idea|./.idea/*|./.qoder|./.qoder/*|./.claude|./.claude/*|./.codex|./.codex/*)
      return 0 ;;
  esac
  return 1
}

remove_path() {
  local path="$1"
  [ -e "$path" ] || [ -L "$path" ] || return 0
  if is_protected "$path"; then
    return 0
  fi
  say "remove $path"
  if [ "$DRY_RUN" -eq 0 ]; then
    rm -rf -- "$path"
  fi
  removed=$((removed + 1))
}

remove_find() {
  local path
  while IFS= read -r -d '' path; do
    remove_path "$path"
  done < <(find . -type d \( -name .git -o -name node_modules -o -name target \) -prune -o "$@" -print0 2>/dev/null)
}

remove_build_outputs() {
  local pom
  # Maven output belongs to the directory declaring the module. A Java package
  # named target or build is ordinary source, including in a new untracked module.
  while IFS= read -r -d '' pom; do
    remove_path "${pom%/pom.xml}/target"
  done < <(find . -type d \( -name .git -o -name src -o -name target -o -name node_modules \) -prune \
    -o -path './pangrova-worker-native' -prune -o -name pom.xml -type f -print0)
  local build_directory
  for build_directory in pangrova-worker-native/build pangrova-worker-native/build-*; do
    [ -d "$build_directory" ] && remove_path "$build_directory"
  done
  return 0
}

# Ephemeral caches produced by ordinary Java, Python, and test runs.
remove_find -name 'javac.*.args' -type f
remove_find -name '.jqwik-database'
remove_find -name 'dependency-reduced-pom.xml'
remove_find -name '*.pyc' -type f
remove_find -name '__pycache__' -type d

# Retired non-published documentation location.
remove_path docs/internal

# A module directory holding nothing but target/ is output left by a deleted module.
while IFS= read -r -d '' output; do
  module="${output%/target}"
  [ -f "$module/pom.xml" ] && continue
  [ "$(ls -A "$module")" = "target" ] && remove_path "$module"
done < <(find . -mindepth 2 -maxdepth 3 -path './pangrova-*' -type d -name target -print0 2>/dev/null)

if [ "$ALL" -eq 1 ]; then
  remove_build_outputs
  remove_path pangrova-worker-native/deps-download
  remove_path pangrova-worker-native/deps-install
  remove_path logs
  remove_path native-output
  remove_path native-lib
  remove_path output
fi

say "Cleanup complete: $removed path(s) processed."
