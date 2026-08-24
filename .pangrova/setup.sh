#!/usr/bin/env bash
# Verify or restore thin tool adapters to the canonical .pangrova rules.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
  CHECK_ONLY=true
elif [[ $# -gt 0 ]]; then
  printf 'Usage: %s [--check]\n' "$0" >&2
  exit 2
fi

ensure_link() {
  local link_path="$1"
  local target="$2"
  local display_path="${link_path#"$PROJECT_ROOT/"}"

  if [[ -L "$link_path" && "$(readlink "$link_path")" == "$target" ]]; then
    printf 'OK %s -> %s\n' "$display_path" "$target"
    return
  fi

  if $CHECK_ONLY; then
    printf 'ERROR %s must link to %s\n' "$display_path" "$target" >&2
    return 1
  fi

  if [[ -e "$link_path" || -L "$link_path" ]]; then
    printf 'ERROR refusing to replace existing non-matching path: %s\n' "$display_path" >&2
    return 1
  fi

  mkdir -p "$(dirname "$link_path")"
  ln -s "$target" "$link_path"
  printf 'CREATED %s -> %s\n' "$display_path" "$target"
}

[[ -d "$PROJECT_ROOT/.pangrova/rules" ]] || {
  printf 'ERROR canonical rules directory is missing\n' >&2
  exit 1
}

ensure_link "$PROJECT_ROOT/.claude/rules" "../.pangrova/rules"
ensure_link "$PROJECT_ROOT/.qoder/rules" "../.pangrova/rules"

printf 'Agent workspace adapters are valid.\n'
