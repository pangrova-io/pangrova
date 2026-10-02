#!/usr/bin/env bash
# Prepare the exact repository-owned SDK writer drain extension, never mutate
# Cargo's registry. All unmodified crate files are also provenance-checked.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patch_file="$script_dir/../patches/lance-writer-drain.patch"
version=0.20.0
crates=(lance-io lance-file lance-encoding lance)
archives=(
  c17e6739ec725950dbe77814d34b03bd29a03c1807c1406994fcf57ff87f94ef
  b2f5a12060b6a1626a52fc7821d53a9f1e9c7d0b87cfaa4a9b08e5a7762ed98a
  8057c9ca7f3d84640514531c5dfe81fb288a24e414f086284c8bac8b3539e650
  cceab54ac337e206bab80de6438f036fa26c84b4721a45efdbd08be0bd2f9e30
)
trees=(
  4d37b8acef263e71a0bcbf2e376f7348c8874cdff64bf92a5cc880a19f54b421
  58b5cfa12fbf3b84c6deee0162df110cd95cbbf9aa149bebf0538d74c9d59749
  f545f27491068ce3d20107ccd894b6a467f7bd49f947bd8e21411ee52fc9d36c
  7c7ced7068ba8d4c51e9c453d05ff0757e618f29dda4966e47abd2e617c7c68d
)
sources=(
  lance-io-0.20.0/src/object_writer.rs
  lance-file-0.20.0/src/v2/writer.rs
  lance-encoding-0.20.0/src/encodings/logical/primitive.rs
  lance-encoding-0.20.0/src/encodings/logical/list.rs
  lance-io-0.20.0/src/object_store.rs
  lance-io-0.20.0/src/scheduler.rs
  lance-encoding-0.20.0/src/compression_algo/fastlanes.rs
  lance-encoding-0.20.0/src/decoder.rs
  lance-encoding-0.20.0/src/encodings/logical/binary.rs
  lance-encoding-0.20.0/src/encodings/logical/blob.rs
  lance-encoding-0.20.0/src/encodings/logical/struct.rs
  lance-0.20.0/src/dataset.rs
  lance-0.20.0/src/dataset/write.rs
  lance-0.20.0/src/dataset/updater.rs
  lance-0.20.0/src/dataset/write/merge_insert.rs
  lance-0.20.0/src/dataset/fragment/write.rs
  lance-file-0.20.0/src/writer.rs
)
before=(
  7c8a296cafe7ad6a13d6aca5d3bc230aecc723c64329b9e0b6e0f56f3dd57a20
  b15c4652e2c1a85360dd446b4eecbd7a06c0b972ae9bf871998727c223e0193e
  5e7f11feff373990928cce75375747b349026695f6a5b1ca6bf8af7654e13baf
  7d8aaf1410a5500abe398670f14d2d4b4c0570537481e3c7814b96b6ccaa52d6
  a7eb96a086820647d21f9efb2d9bb58fd056e06c7c5c8139270d6621470b5914
  11c1b92026a65e91b2dca868fea7faf35dd067065d93e481d1858d4aa5dfea53
  eb73f4de47ae3fc692eacdb6246682bf5a49835b459a18fb0922291088df170e
  d83b6d13fc0a761039b15a2d67fe887691010c229e2ca6e53277d36fdae369a0
  372b4ddbc5c8436386c9e2127ae34b134d8a8098584ba80977bc68729d70861a
  ebad2dc088224f5a59ce9c7b8908b01a4ddfbdd17c9c5d6c12b330b731008fcc
  5d1e7e7d05110a0004a22d4f6f33ecbdd94471c7ad8571ace97979880708bc63
  92aefb2c8fcab3e8f17232922028b40a007658246c81da1a8e45fb0b9294e2b4
  455193cc60b1070c2c63ff5f82f8d2e62e3c9537341e1c1a3de9a13f679d8542
  81a43f28eaf90b4d19dfbd8d37f48532c3036c92153e60d52786553e844c9193
  e6b26dd13eb622d8ade372add1952b8413b326c0d745264bbb0defa3d756dc1a
  5024d0c90447741b701118fa3fbddcb7dfccca71750f98f32a934d8b67438092
  af1f96325077d8a201f3ffb6def8965db8f23a012e8bec06ff0d79c801d52608
)
after=(
  4cd56afd12a57380b753ce829978f837d63e30961af2c14cf459b1e084efffc6
  09e11e9bd2d02bc48623c6a0bef8a64e96dbe2238a76bec85785a2f9962ca354
  39ccca78ecf3d19822393a77add39585dec2624f741b7e653b88082c3e4a25cb
  5a11c0de38039711face1e551242e9c02200bbc79a3a89143dc205740431a3f4
  d1cac8fc7b0ca21368bd0b4d8bebcf27cbb27eeb5434dcff28ae5854080e60d6
  b6f902dd65305c056955b04d99cba6b4da8b172a23b64f22559a7eb54493b624
  2b59609139aabd6507b42a0c6b00aa05bfab1d3d5eecdace36d49fdafcb3a659
  7bd6e9e70de01b0cfcc9baf437732874868c230d93a81280340ea9bb456fabf9
  dade4ca0eeca83079c5ec09c1061b9b54a59e4a88890813df64d1e083c5dd0f8
  8ebffa42a7d884a3b2b2f2479896f7c29e6afc6f471824666aeb26af2dfe265f
  d5c0a72b227e9bf21cd48cfcee6e438b4ed6f295ceeb3db23bb927e09f82c612
  eed90b65b38e1281fd1853965c80b18cda77724b6813fb962707c2559f0dcb6a
  701acd68ecdf37c7e0ae546499720e3f5df0bac041de300d14cd6811eb85b167
  6746fb627f9605cfda176289ae61f182ac72bf8cfc37c355aa6edb6b1bfa3fff
  4694896caab147347fcee1ac4c42c3465bafefc61f3510b776afca0e65be2b52
  99822feec4959a8f54502334ca30cf5078f5c7d3242be80dcf6845d9f8cec5ce
  1af8724e2a1a441f342e5620023eabf63e0b3c4068b69d0a26c05fcb973e27df
)

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
digest() { openssl dgst -sha256 "$1" | awk '{print $NF}'; }
tree_digest() {
  (
    cd "$1"
    LC_ALL=C rg --files --hidden --no-ignore | LC_ALL=C sort |
      while IFS= read -r source; do printf '%s %s\n' "$(digest "$source")" "$source"; done
  ) | openssl dgst -sha256 | awk '{print $NF}'
}
apply_patch_file() {
  local source_dir="$1"
  shift
  # Prevent discovering the containing application checkout as this SDK's root.
  GIT_CEILING_DIRECTORIES="$(dirname "$source_dir")" git -C "$source_dir" apply "$@" "$patch_file"
}
verify_source() {
  local source_dir="$1" index
  for index in "${!sources[@]}"; do
    [[ -f "$source_dir/${sources[$index]}" &&
       "$(digest "$source_dir/${sources[$index]}")" == "${after[$index]}" ]] ||
      die "Reviewed SDK writer source differs: ${sources[$index]}"
  done
  for index in "${!crates[@]}"; do
    [[ "$(tree_digest "$source_dir/${crates[$index]}-$version")" == "${trees[$index]}" ]] ||
      die "SDK crate includes an unreviewed modification: ${crates[$index]}"
  done
  apply_patch_file "$source_dir" --reverse --check
}
obtain_archive() {
  local crate="$1" expected="$2" output="$3" cached
  local cache_root="${CARGO_HOME:-$HOME/.cargo}/registry/cache"
  for cached in "$cache_root"/*/"$crate-$version.crate"; do
    if [[ -f "$cached" && "$(digest "$cached")" == "$expected" ]]; then
      cp "$cached" "$output"
      return
    fi
  done
  curl --fail --location --silent --show-error \
    "https://static.crates.io/crates/$crate/$crate-$version.crate" --output "$output"
  [[ "$(digest "$output")" == "$expected" ]] || die "Pinned SDK archive checksum differs: $crate"
}

verify_upgrade_base() {
  local source_dir="$1" index
  local prior_trees=(
    4d37b8acef263e71a0bcbf2e376f7348c8874cdff64bf92a5cc880a19f54b421
    1cebdb2727a2ee344bcff1b97281a2819a3f90ff14aba340d2024112a273b519
    f545f27491068ce3d20107ccd894b6a467f7bd49f947bd8e21411ee52fc9d36c
  )
  [[ "$(find "$source_dir" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')" == 6 ]] ||
    die "SDK upgrade base has additional or missing entries"
  for index in "${!prior_trees[@]}"; do
    [[ "$(tree_digest "$source_dir/${crates[$index]}-$version")" == "${prior_trees[$index]}" &&
       "$(digest "$source_dir/${crates[$index]}-$version.crate")" == "${archives[$index]}" ]] ||
      die "SDK upgrade refuses unreviewed prior source: ${crates[$index]}"
  done
}

[[ $# == 2 ]] || die "Usage: prepare-lance-sdk.sh prepare|verify|upgrade DESTINATION"
action="$1"
destination="$2"
[[ "$destination" == /* && "$destination" != / ]] || die "SDK destination must be an explicit absolute directory"
[[ -f "$patch_file" ]] || die "Repository-owned SDK patch is missing"
for required in git openssl rg tar; do command -v "$required" >/dev/null || die "Required tool is missing: $required"; done

case "$action" in
  verify) verify_source "$destination" ;;
  prepare|upgrade)
    if [[ -e "$destination" ]]; then
      [[ -d "$destination" ]] || die "SDK destination already exists and is not a directory"
      if [[ "$action" == prepare || -d "$destination/lance-$version" ]]; then
        verify_source "$destination"
        exit 0
      fi
      verify_upgrade_base "$destination"
      backup="$destination.sdk3-fa6e521783fa1e35"
      [[ ! -e "$backup" ]] || die "Validated upgrade backup already exists; refusing overwrite"
    fi
    mkdir -p "$(dirname "$destination")"
    staging="$(mktemp -d "$destination.prepare.XXXXXX")"
    trap 'rm -rf "$staging"' EXIT
    for index in "${!crates[@]}"; do
      archive="$staging/${crates[$index]}-$version.crate"
      obtain_archive "${crates[$index]}" "${archives[$index]}" "$archive"
      LC_ALL=C tar -xzf "$archive" -C "$staging"
    done
    for index in "${!sources[@]}"; do
      [[ "$(digest "$staging/${sources[$index]}")" == "${before[$index]}" ]] ||
        die "Pinned SDK source does not match reviewed patch base: ${sources[$index]}"
    done
    apply_patch_file "$staging" --check
    apply_patch_file "$staging"
    verify_source "$staging"
    # Preserve the exact verified historical input for recovery. No consumer
    # uses it; the only live path is the newly verified canonical destination.
    if [[ -e "$destination" ]]; then
      verify_upgrade_base "$destination"
      mv "$destination" "$backup"
      if ! mv "$staging" "$destination"; then
        mv "$backup" "$destination"
        die "SDK publication failed; restored its verified prior source"
      fi
    else
      [[ ! -e "$destination" ]] || die "SDK destination appeared concurrently"
      mv "$staging" "$destination"
    fi
    verify_source "$destination"
    ;;
  *) die "Unknown SDK operation: $action" ;;
esac
