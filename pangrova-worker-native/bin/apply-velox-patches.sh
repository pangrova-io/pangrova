#!/usr/bin/env bash
# Apply/verify the sole pinned SDK ownership, selection and Arrow execution patch.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patch_file="$script_dir/../patches/velox-function-metadata-snapshot.patch"
pinned_commit=3da0390118fa4b0addbfbdeb03f438ec1b1237a6
proof_version=pangrova-function-provenance-v2
contract_tools="$script_dir/../../pangrova-protocol/tools/function-contracts"
contract_declaration="$script_dir/../../pangrova-protocol/src/main/resources/function-contracts/execution-selection.json"
source_files=(\
  velox/expression/SimpleFunctionRegistry.h
  velox/expression/SimpleFunctionRegistry.cpp
  velox/vector/BaseVector.h
  velox/vector/BaseVector.cpp
  velox/vector/arrow/Bridge.cpp
  velox/exec/RowContainer.cpp
  velox/core/PlanNode.h
  velox/core/PlanNode.cpp
  velox/exec/NestedLoopJoinProbe.h
  velox/exec/NestedLoopJoinProbe.cpp
  velox/exec/PartitionedOutput.cpp
  velox/vector/VectorStream.h
  velox/vector/VectorStream.cpp
  velox/expression/SpecialFormRegistry.h
  velox/expression/SpecialFormRegistry.cpp
  velox/expression/VectorFunction.cpp
  velox/core/QueryConfig.h
  velox/core/QueryConfig.cpp
  velox/expression/ExprCompiler.cpp
  velox/expression/FunctionCallToSpecialForm.cpp
  velox/exec/Aggregate.cpp
  velox/exec/WindowFunction.cpp
  velox/exec/WindowFunction.h
  velox/exec/Window.cpp
  velox/expression/CMakeLists.txt
  velox/exec/CMakeLists.txt
  velox/expression/FunctionRegistrySnapshot.h
  velox/expression/FunctionRegistryMutation.h
  velox/expression/FunctionRegistrySnapshot.cpp
  velox/exec/FunctionRegistrySnapshot.cpp
  velox/exec/Exchange.cpp
  velox/functions/FunctionRegistry.cpp
  velox/exec/Aggregate.h
  velox/exec/AggregateCompanionAdapter.cpp
  velox/exec/AggregateCompanionAdapter.h
  velox/exec/AggregateWindow.cpp
  velox/exec/AggregateWindow.h
  velox/exec/FunctionRegistrySnapshotBindings.cpp
  velox/expression/CastExpr.cpp
  velox/expression/CastExpr.h
  velox/expression/CoalesceExpr.cpp
  velox/expression/CoalesceExpr.h
  velox/expression/ConjunctExpr.cpp
  velox/expression/ConjunctExpr.h
  velox/expression/FunctionCallToSpecialForm.h
  velox/expression/RowConstructor.cpp
  velox/expression/RowConstructor.h
  velox/expression/SwitchExpr.cpp
  velox/expression/SwitchExpr.h
  velox/expression/TryExpr.cpp
  velox/expression/TryExpr.h
  velox/expression/VectorFunction.h
  velox/expression/FunctionRegistryIdentity.cpp
  velox/expression/FunctionRegistryIdentity.h
  velox/exec/OperatorUtils.h
  velox/exec/OperatorUtils.cpp
  velox/exec/Merge.cpp
  velox/vector/arrow/Bridge.h
  CMake/resolve_dependency_modules/duckdb.cmake
  scripts/setup-macos.sh
  velox/exec/AggregateInfo.cpp
  velox/exec/ColumnStatsCollector.cpp
  velox/exec/AggregateFunctionRegistry.cpp
  velox/exec/AggregateFunctionRegistry.h
  velox/functions/prestosql/types/UuidType.h
  velox/functions/prestosql/types/UuidRegistration.cpp)
base_blobs=(\
  77a3acb529dda19faf155e3e277956b5ce52faa7
  2fa36c309dec1fd30ff18f5ec9df54bc1bc9b82d
  16fb99fc09d3deb28d8a2b7c6515a1fda38d5d70
  454e72bf2aa1559d0b8e194abda4a8ee20766e53
  f980599e79a709e50941156f3398f75c43d1c96a
  5a3e8e291e05bfa52f8b0c2bedc4bcf069ffa7d7
  4a1ba1579cde2cf7619fa6378ad7ae8f5533112a
  ac399190e815639816e3130cd93fe89e6b2b3dd2
  cbec6cc4dae04816040eeca784360cad0e8da7df
  868e8088920b242d1c6ef6a2a6821412e303a6a6
  ba4e23d738b1e70a7b6541e694e8ea11d2febd24
  7da701b1a9e7d68f8512fd6ff8a9d0d3ec416922
  b257436dfd5be7ebc10331a4fad8a042e70b05b0
  167d37981750bc2178c1942c7b0f73ef0dbdab2a
  dbfeb4b2c6bac8b5b46ce27fcc1b0a55ad37df4a
  08cf3c5c31e599d5719542d0bf2ff009a2454f11
  772015daa1f5886423365a1c174de6dad3a51077
  4a31862590ad4014e147fe2c58582d6c7738fe9d
  9a8e96f7089c3a45c328c3db73498a0efca127ce
  c23fbd8ba75f36d00db1b38fab9077dcd3e21761
  c48cfe47a9169912e1bd81b3afca893f45f008b7
  3c2d31cf62c31a0bd96c1e526612b0ae2191a805
  7f997dfc911831c924218bd9830b86f502c9472d
  f9107522f0a3bdf32ba675510050a5e80b7e08b5
  5983d146b628949a2f29d170acc0e83049be47e5
  3a5bec7e6e8760e9a041640a7e85f3cf3eabdfbb
  ABSENT
  ABSENT
  ABSENT
  ABSENT
  c72cfeada07c9e0e6e448cfd78a9782cf954c6cd
  a76b738eb6e21192f186eb055ab85dc65d4f76fc
  d38f91bc3af027bc128e9048b5b681d7446b0c1c
  9aab4fbac9b0ec815ea98b7ed3e76f3b6b4af991
  fc0a5909f8a69813808bd10cebabfb4534b9c0bc
  6c90a4fd1d5a6af07b3e84ae35e6f2879b5fa37e
  09bb55f06199c45acea6f073dfd068e83f74dfd2
  ABSENT
  1fa4dd7e14cc090938f2df7eb3645314a99f6fbb
  9a08ceaba41f82f348ffff3f7963b562ff7ca186
  4f7023aee328e871b456e1405fea0a8221ecdc3c
  8afbcdfffb6baf7f7761540ea032b7e82348bc53
  1dd98b46120750d0a1a5736a886008be72b8c111
  ba3135f11103cb4dc22998956e9049724a0b9308
  9da39b70edd6358ba9e43394797b65cbaeae03d9
  323acac23666899922f3dac1fbb87726d73aceee
  dd84d26ca973c1c5981f165e742633c00b2a9d10
  500ea7c665bf11a5976713183397c7aa9a87544f
  15ac6ca0514c2587b9448bfece2913db382807c1
  174947c3abd2a952f90a99b38e827d3d8772a64b
  d248f9dcef87174ecbe301406c8bf8f218b27515
  cbe213947ede61339db7371212bbb7d6993e01d6
  ABSENT
  ABSENT
  909a208611d496cce513ae3b571f82f0d17ffb3e
  c4ed55d7716c9c9f13e461984ff6cca8accc27e7
  0f42d3dcac42aafa3de1f84142ef61e5750378ff
  278d82d0122cd2723df3173456b4e9c700109328
  bd3fbd7e14f8afc7f7377a3efd7142a49c495fe7
  aded94e0fb4ca19fab31ce8e9c819cf61cfa60d6
  044a510e848c7587c74215dcd45ad355ef0a25d6
  1ec85678924b640fc18cd83c015c799bdc1ed4fa
  1122218c3600f0976b6f0d82de7f693227ba53a9
  4adea534887b43e4a12f74b1f08647eba42d8e07
  c6bd305434a5dd78322fab7d646aea6c25f7f47b
  39797eeef29429ab0976a8b35299822b497283f2)
prior_blobs=(\
  08c31f1bd17085dd98c8a986ff046a660b6893db
  650df1062c638a358291060fc634c2ace82af533
  f8d7e9600c182955a9b13fff2845348072c4d28b
  26ca432896b1aa049be643062d49cb65994964b2
  92fb113e97222aa5fc31c59443d8486652d78604
  ae843dcab73e3918ef9f8820c979b238710b2f61
  2585c367f6da90bc4bb232da48fa31da0da3ab85
  be770ab32470268a7d803becd4a6b6d1324dca25
  682197119052d3c37fdb1a3107f43faec5ef4f7c
  de710ca54e747ffaea22d8fa0118675103546723
  2b6b2f26ee9390364ee596d3d718ed9c073dd40c
  f6bca6e8ad2e1a751433b85236fbcb532a0b50bd
  9674307c95af9a08ba47b2af3d4d1381cf40c620
  3032d1783035b34ecf06fcc8daddfb5d68b476fd
  7e7e53501baa67233c88de429cf309e8d22ed7f6
  d6abd93b9d1768eb49658e1ad2adc7c9956b53c0
  272fde3930aabee45f50ca5c74c133dc9e93e4e5
  ff1f8c7b28e234ff5e1d504916c7952f46305f0a
  17b58aeec67e501662396e1803471a262b74fae6
  da125d202ff3ff4cbc975fdf4079b76c14db8176
  ab59a62db58f437c4f540e4f3c5217f4259c3480
  a6e95694650c79e7b958a976d16d5bb5eb6ec24a
  c4c1ee246cfe80d2623c921cc5db2e31ec594c5b
  04b2d3c87f9dfeb3fe665bef1e45fe74f766bf82
  4c8e176bc60b57d53a73240caebe5045e5cd5aac
  41b70e6439f91a902327b5d0ea73928e4b38b30b
  9e18aaffe2654d1b7fcb9eff2114c6e15d343739
  559a593f1cc977e4ef8d5ea71235f16863fc0a7c
  c9d2bccf9e650e4fda328160d02f84263644e849
  2250fe6d85ab5309895afe58d60c8e9b73542346
  92a3c8d96710c49fbe1810b5f4cd5df17d2a3c0e
  d86a5434a749972c146567708cf66dd5231811c8
  5b10a60a47a66591adc4e7aa08432778c05dbfb1
  ea7d56a551c1c5919c496895310e1102181255a1
  2251ee3ca370c8b0d6d637970c8df7e1d54a157c
  97f6e9324c11f6dc107ac00f5b0b1294cf8ce883
  c3252547b2892a8fe01a4a92aca75f3c36e1df46
  dc4991ad52266798de66eac75649825b69de58c2
  ae849762bd3a0964d9cde69f6551e716153bf687
  426f8fa7049f7fb5fd55c653a65c3ed4fce993ce
  6534182e2fd9026fde8dd0e74bdd547cc5c3eda0
  7331a29d5065bd610998e5a4109a138009a8dc53
  c680ccdc2e7e1c25546b9854ee69a8d239f0cac4
  0002bcd35ffae82de81ed6cb81cca6ecf92fdcb9
  9c2d1466a9d4e31da4e9683002564e749f6598dd
  afab5d76ddcca3ea18291454fd7224e6fb949978
  0d8bce648a4275797e9ccc3b703667b2f6bf09aa
  c57b144bc15ed2e74d11ad6e48af54551b84f00f
  c06be0a386dc9b1c200753ee98a3b7b3cab7f043
  7350defbf51d7a9f82caa3061230c823a7f52f59
  a1f6c07696510b4b7d303a39fe29a26ed0a979ff
  7d380506175f1622ccb293e9d0a759e0cfc43f23
  584687f931f432ecfce3a080b70d7cb1b6f184e1
  27ca5c805461591bb7d0618c471b213c0ffefe71
  7ab49db75e49f73ca4d2b7ba39f1f2180684cfe3
  bd763da3678e28d0a0ed4970fdd20ee7e412c827
  a1955e70aa55d5b641a432fec0f504bd3b867042
  1175121782d19a2dc5fabfa3625de2db0c989543
  3a2b866cf71205f9fb0c994db7dcc6d39365bce4
  c910e8dde2fda95285767e4ff4d2d9012fc345a2
  7020e357610a2c91b42c0308afc65e47c756b6b0
  6e8bc195e5bfaf39af9a9151d6dd1c796dece931
  a73329db62f250a96866865bce653bc8a373b8d7
  24150367534e6636e7cb56dcd23cca44b1188e21
  c6bd305434a5dd78322fab7d646aea6c25f7f47b
  39797eeef29429ab0976a8b35299822b497283f2)
patched_blobs=(\
  08c31f1bd17085dd98c8a986ff046a660b6893db
  650df1062c638a358291060fc634c2ace82af533
  f8d7e9600c182955a9b13fff2845348072c4d28b
  26ca432896b1aa049be643062d49cb65994964b2
  53d51844e9b1f8097ef5cc197fba2191c4f823bb
  ae843dcab73e3918ef9f8820c979b238710b2f61
  2585c367f6da90bc4bb232da48fa31da0da3ab85
  be770ab32470268a7d803becd4a6b6d1324dca25
  682197119052d3c37fdb1a3107f43faec5ef4f7c
  de710ca54e747ffaea22d8fa0118675103546723
  2b6b2f26ee9390364ee596d3d718ed9c073dd40c
  f6bca6e8ad2e1a751433b85236fbcb532a0b50bd
  9674307c95af9a08ba47b2af3d4d1381cf40c620
  3032d1783035b34ecf06fcc8daddfb5d68b476fd
  7e7e53501baa67233c88de429cf309e8d22ed7f6
  d6abd93b9d1768eb49658e1ad2adc7c9956b53c0
  272fde3930aabee45f50ca5c74c133dc9e93e4e5
  ff1f8c7b28e234ff5e1d504916c7952f46305f0a
  17b58aeec67e501662396e1803471a262b74fae6
  da125d202ff3ff4cbc975fdf4079b76c14db8176
  ab59a62db58f437c4f540e4f3c5217f4259c3480
  a6e95694650c79e7b958a976d16d5bb5eb6ec24a
  c4c1ee246cfe80d2623c921cc5db2e31ec594c5b
  04b2d3c87f9dfeb3fe665bef1e45fe74f766bf82
  4c8e176bc60b57d53a73240caebe5045e5cd5aac
  41b70e6439f91a902327b5d0ea73928e4b38b30b
  9e18aaffe2654d1b7fcb9eff2114c6e15d343739
  559a593f1cc977e4ef8d5ea71235f16863fc0a7c
  c9d2bccf9e650e4fda328160d02f84263644e849
  2250fe6d85ab5309895afe58d60c8e9b73542346
  92a3c8d96710c49fbe1810b5f4cd5df17d2a3c0e
  d86a5434a749972c146567708cf66dd5231811c8
  5b10a60a47a66591adc4e7aa08432778c05dbfb1
  ea7d56a551c1c5919c496895310e1102181255a1
  2251ee3ca370c8b0d6d637970c8df7e1d54a157c
  97f6e9324c11f6dc107ac00f5b0b1294cf8ce883
  c3252547b2892a8fe01a4a92aca75f3c36e1df46
  dc4991ad52266798de66eac75649825b69de58c2
  ae849762bd3a0964d9cde69f6551e716153bf687
  426f8fa7049f7fb5fd55c653a65c3ed4fce993ce
  6534182e2fd9026fde8dd0e74bdd547cc5c3eda0
  7331a29d5065bd610998e5a4109a138009a8dc53
  c680ccdc2e7e1c25546b9854ee69a8d239f0cac4
  0002bcd35ffae82de81ed6cb81cca6ecf92fdcb9
  9c2d1466a9d4e31da4e9683002564e749f6598dd
  afab5d76ddcca3ea18291454fd7224e6fb949978
  0d8bce648a4275797e9ccc3b703667b2f6bf09aa
  c57b144bc15ed2e74d11ad6e48af54551b84f00f
  c06be0a386dc9b1c200753ee98a3b7b3cab7f043
  7350defbf51d7a9f82caa3061230c823a7f52f59
  a1f6c07696510b4b7d303a39fe29a26ed0a979ff
  7d380506175f1622ccb293e9d0a759e0cfc43f23
  584687f931f432ecfce3a080b70d7cb1b6f184e1
  27ca5c805461591bb7d0618c471b213c0ffefe71
  7ab49db75e49f73ca4d2b7ba39f1f2180684cfe3
  bd763da3678e28d0a0ed4970fdd20ee7e412c827
  a1955e70aa55d5b641a432fec0f504bd3b867042
  1175121782d19a2dc5fabfa3625de2db0c989543
  3a2b866cf71205f9fb0c994db7dcc6d39365bce4
  c910e8dde2fda95285767e4ff4d2d9012fc345a2
  7020e357610a2c91b42c0308afc65e47c756b6b0
  6e8bc195e5bfaf39af9a9151d6dd1c796dece931
  a73329db62f250a96866865bce653bc8a373b8d7
  24150367534e6636e7cb56dcd23cca44b1188e21
  0d622555a73c5c587c6253e5714d592031780e50
  820e77cbcc4abe8236ec0ba7e028a45880eece23)

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
digest() { openssl dgst -sha256 "$1" | awk '{print $NF}'; }

[[ $# -ge 2 ]] || die "Usage: apply-velox-patches.sh apply|verify|record SOURCE [ARCHIVE IDENTITY_INSPECTOR]"
action="$1"
source_dir="$2"
[[ ${#source_files[@]} == ${#base_blobs[@]} &&
   ${#source_files[@]} == ${#patched_blobs[@]} &&
   ${#source_files[@]} == ${#prior_blobs[@]} ]] || \
  die "Reviewed SDK source and checkpoint blob tables must have identical cardinality"
[[ -d "$source_dir/.git" && -f "$patch_file" ]] || die "Pinned source or patch is missing"
[[ "$(git -C "$source_dir" rev-parse HEAD)" == "$pinned_commit" ]] || \
  die "Velox patch requires the exact pinned source commit"
for index in "${!source_files[@]}"; do
  if [[ "${base_blobs[$index]}" == ABSENT ]]; then
    [[ -z "$(git -C "$source_dir" ls-tree HEAD -- "${source_files[$index]}")" ]] || \
      die "New owned SDK source unexpectedly exists in the pinned base"
  else
    [[ "$(git -C "$source_dir" rev-parse "HEAD:${source_files[$index]}")" == "${base_blobs[$index]}" ]] || \
      die "Pinned source blob differs from reviewed base: ${source_files[$index]}"
  fi
done

matches_blob() {
  local file="$1" expected="$2"
  if [[ "$expected" == ABSENT ]]; then
    [[ ! -e "$source_dir/$file" ]]
  else
    [[ -f "$source_dir/$file" && "$(git -C "$source_dir" hash-object "$file")" == "$expected" ]]
  fi
}

has_blobs() {
  local state="$1" index expected
  for index in "${!source_files[@]}"; do
    case "$state" in
      base) expected="${base_blobs[$index]}" ;;
      prior) expected="${prior_blobs[$index]}" ;;
      patched) expected="${patched_blobs[$index]}" ;;
      *) die "Unknown reviewed SDK source state: $state" ;;
    esac
    matches_blob "${source_files[$index]}" "$expected" || return 1
  done
}

verify_source() {
  has_blobs patched || die "SDK source lacks the complete patch or has unreviewed drift"
  git -C "$source_dir" apply --unidiff-zero --reverse --check "$patch_file"
}

prior_upgrade_patch() {
  # Upgrade only the complete, reviewed source checkpoint immediately preceding
  # the canonical Arrow UUID bridge. Reject partial or unknown drift.
  cat <<'PANGROVA_PRIOR_UPGRADE'
--- a/velox/vector/arrow/Bridge.cpp
+++ b/velox/vector/arrow/Bridge.cpp
@@ -22,0 +23,2 @@
+#include "velox/functions/prestosql/types/UuidType.h"
+#include "velox/type/DecimalUtil.h"
@@ -29,0 +32 @@
+#include <cstring>
@@ -37,0 +41,62 @@
+
+// Arrow C Data Interface metadata is a native-endian length-prefixed map.
+// This immutable canonical declaration outlives every exported schema.
+const std::string& uuidExtensionMetadata() {
+  static const std::string metadata = [] {
+    std::string result;
+    const auto integer = [&](int32_t value) {
+      result.append(reinterpret_cast<const char*>(&value), sizeof(value));
+    };
+    const auto text = [&](std::string_view value) {
+      integer(static_cast<int32_t>(value.size()));
+      result.append(value);
+    };
+    integer(2);
+    text("ARROW:extension:name");
+    text("arrow.uuid");
+    text("ARROW:extension:metadata");
+    text("");
+    return result;
+  }();
+  return metadata;
+}
+
+bool isArrowUuid(const ArrowSchema& schema) {
+  if (!schema.metadata) return false;
+  const char* cursor = schema.metadata;
+  // The C Data Interface passes a valid producer-owned metadata buffer without
+  // its length. Preserve its standard int32 framing; do not impose private
+  // limits on otherwise valid schemas.
+  const auto integer = [&]() {
+    int32_t value;
+    std::memcpy(&value, cursor, sizeof(value));
+    cursor += sizeof(value);
+    return value;
+  };
+  const auto text = [&]() {
+    const auto size = integer();
+    VELOX_USER_CHECK_GE(size, 0, "Invalid Arrow metadata length");
+    std::string_view value(cursor, size);
+    cursor += size;
+    return value;
+  };
+  const auto count = integer();
+  VELOX_USER_CHECK_GE(count, 0, "Invalid Arrow metadata pair count");
+  std::optional<std::string_view> name, payload;
+  for (int32_t i = 0; i < count; ++i) {
+    const auto key = text();
+    const auto value = text();
+    if (key == "ARROW:extension:name") {
+      VELOX_USER_CHECK(!name.has_value(), "Duplicate Arrow extension name");
+      name = value;
+    } else if (key == "ARROW:extension:metadata") {
+      VELOX_USER_CHECK(!payload.has_value(), "Duplicate Arrow extension metadata");
+      payload = value;
+    }
+  }
+  if (name != "arrow.uuid") return false;
+  VELOX_USER_CHECK(schema.format && std::string_view(schema.format) == "w:16",
+      "Arrow UUID requires fixed-size binary(16)");
+  VELOX_USER_CHECK(!payload || payload->empty(), "Arrow UUID metadata must be empty");
+  return true;
+}
@@ -280,0 +346 @@
+  if (isUuidType(type)) return "w:16";
@@ -797,3 +863,3 @@
-  // Timestamp needs layout conversion. Millisecond TIME narrows int64_t to
-  // Arrow time32; microsecond TIME already has Arrow time64's exact layout.
-  const bool narrowsTime = type->equivalent(*TIME());
+  // UUID converts to big-endian bytes. Millisecond TIME narrows to time32;
+  // microsecond TIME already has Arrow time64's exact layout.
+  const bool convertsLayout = type->equivalent(*TIME()) || isUuidType(type);
@@ -801,3 +867,3 @@
-    return !type->isTimestamp() && !narrowsTime;
-  }
-  return !type->isShortDecimal() && !type->isTimestamp() && !narrowsTime;
+    return !type->isTimestamp() && !convertsLayout;
+  }
+  return !type->isShortDecimal() && !type->isTimestamp() && !convertsLayout;
@@ -844 +910,9 @@
-  if (type->kind() == TypeKind::TIMESTAMP) {
+  if (isUuidType(type)) {
+    const auto* input = vec.as<FlatVector<int128_t>>();
+    auto* output = values->asMutable<int128_t>();
+    vector_size_t ordinal = 0;
+    rows.applyWithVisibility([&](vector_size_t row, bool hidden) {
+      output[ordinal++] = hidden || input->isNullAt(row)
+          ? 0 : DecimalUtil::bigEndian(input->valueAt(row));
+    });
+  } else if (type->kind() == TypeKind::TIMESTAMP) {
@@ -1509,0 +1584 @@
+  if (isArrowUuid(arrowSchema)) return UUID();
@@ -1701,0 +1777 @@
+      if (isUuidType(type)) valuesChild->metadata = uuidExtensionMetadata().data();
@@ -1794,0 +1871,3 @@
+  if (isUuidType(type) && std::string_view(arrowSchema.format) == "w:16") {
+    arrowSchema.metadata = uuidExtensionMetadata().data();
+  }
@@ -2395,0 +2475,22 @@
+  if (isUuidType(type)) {
+    VELOX_USER_CHECK_EQ(arrowArray.n_buffers, 2, "Arrow UUID requires validity and value buffers");
+    VELOX_USER_CHECK_LE(arrowArray.length, std::numeric_limits<vector_size_t>::max(),
+        "Arrow UUID exceeds vector row bound");
+    const auto length = static_cast<vector_size_t>(arrowArray.length);
+    auto values = AlignedBuffer::allocate<int128_t>(length, pool);
+    auto* output = values->asMutable<int128_t>();
+    const auto* input = static_cast<const uint8_t*>(arrowArray.buffers[1]);
+    VELOX_USER_CHECK(input || length == 0 || arrowArray.null_count == length,
+        "Arrow UUID values buffer is missing");
+    for (vector_size_t row = 0; row < length; ++row) {
+      if (nulls && bits::isBitNull(nulls->as<uint64_t>(), row)) {
+        output[row] = 0;
+        continue;
+      }
+      int128_t encoded;
+      std::memcpy(&encoded, input + row * sizeof(encoded), sizeof(encoded));
+      output[row] = DecimalUtil::bigEndian(encoded);
+    }
+    return createFlatVector<TypeKind::HUGEINT>(pool, type, std::move(nulls), length,
+        std::move(values), arrowArray.null_count);
+  }
diff --git a/velox/functions/prestosql/types/UuidType.h b/velox/functions/prestosql/types/UuidType.h
index c6bd305..0d62255 100644
--- a/velox/functions/prestosql/types/UuidType.h
+++ b/velox/functions/prestosql/types/UuidType.h
@@ -28 +28 @@ class UuidType final : public HugeintType {
-  UuidType() = default;
+  constexpr UuidType() : HugeintType{ProvideCustomComparison{}} {}
@@ -35,0 +36,12 @@ class UuidType final : public HugeintType {
+  // Match the unsigned comparisons implemented by UUID scalar functions.
+  // Sorting and aggregate selection must use the same ordering of all 128 bits.
+  int32_t compare(const int128_t& left, const int128_t& right) const override {
+    const auto a = static_cast<uint128_t>(left);
+    const auto b = static_cast<uint128_t>(right);
+    return a < b ? -1 : a > b ? 1 : 0;
+  }
+
+  uint64_t hash(const int128_t& value) const override {
+    return folly::hasher<int128_t>()(value);
+  }
+
diff --git a/velox/functions/prestosql/types/UuidRegistration.cpp b/velox/functions/prestosql/types/UuidRegistration.cpp
index 39797ee..820e77c 100644
--- a/velox/functions/prestosql/types/UuidRegistration.cpp
+++ b/velox/functions/prestosql/types/UuidRegistration.cpp
@@ -46,0 +47,2 @@ struct UuidParser<TypeKind::VARBINARY> {
+    VELOX_USER_CHECK_EQ(
+        uuidValue.size(), 16, "UUID requires exactly 16 binary bytes");

PANGROVA_PRIOR_UPGRADE
}

case "$action" in
  apply)
    if has_blobs base; then
      git -C "$source_dir" apply --unidiff-zero --check "$patch_file"
      git -C "$source_dir" apply --unidiff-zero "$patch_file"
    elif has_blobs prior; then
      prior_upgrade_patch | git -C "$source_dir" apply --unidiff-zero --check
      prior_upgrade_patch | git -C "$source_dir" apply --unidiff-zero
    elif ! has_blobs patched; then
      die "Unsupported partial SDK patch or unreviewed drift; use the exact pinned base or reviewed source checkpoint"
    fi
    verify_source
    ;;
  verify) verify_source ;;
  record)
    verify_source
    [[ $# == 4 && -f "$3" && -x "$4" ]] || die "Recording requires the actual rebuilt archive and its linked identity inspector"
    input_digest="$("${PANGROVA_FUNCTION_PYTHON:-python3}" "$contract_tools/generate_execution_selection.py" \
      --declaration "$contract_declaration" --input-digest-only)"
    [[ "$input_digest" =~ ^[0-9a-f]{64}$ ]] || die "Malformed current generation input identity"
    patch_digest="$(digest "$patch_file")"
    archive_digest="$(digest "$3")"
    provenance_file="$3.pangrova-provenance"
    if [[ -e "$provenance_file" ]]; then
      [[ -f "$provenance_file" ]] || die "Existing provenance is not a regular file"
      {
        IFS= read -r recorded_version || die "Missing provenance format version"
        IFS= read -r recorded_commit || die "Missing provenance commit"
        IFS= read -r recorded_patch || die "Missing provenance patch digest"
        IFS= read -r recorded_inputs || die "Missing provenance generator input digest"
        IFS= read -r recorded_archive || die "Missing provenance archive digest"
        extra_line=""
        if IFS= read -r extra_line || [[ -n "$extra_line" ]]; then
          die "Existing provenance must contain exactly five lines"
        fi
      } < "$provenance_file"
      [[ "$recorded_version" == "$proof_version" && "$recorded_commit" == "$pinned_commit" &&
         "$recorded_patch" =~ ^[0-9a-f]{64}$ &&
         "$recorded_inputs" =~ ^[0-9a-f]{64}$ &&
         "$recorded_archive" =~ ^[0-9a-f]{64}$ ]] || die "Malformed existing provenance"
      # A known identical old archive cannot become a new-patch build merely
      # by changing its record. This is not authentication or behavior proof.
      if [[ ( "$recorded_patch" != "$patch_digest" || "$recorded_inputs" != "$input_digest" ) &&
            "$recorded_archive" == "$archive_digest" ]]; then
        die "Archive is unchanged from the recorded prior patch; rebuild it"
      fi
    fi
    inspection="$("$4")" || die "Actual archive identity inspector failed"
    [[ "$inspection" == "$(printf '%s\n%s\n%s' "$input_digest" "$patch_digest" "$archive_digest")" ]] || \
      die "Actual archive generation inputs, compiled patch or linked archive identity differ; rebuild it"
    nm -g "$3" | awk 'NF >= 2 && $(NF-1) ~ /^[TW]$/ && $NF ~ /getFunctionMetadataSnapshot/ { found=1 } END { exit !found }' || \
      die "Archive does not define the actual registration snapshot API; rebuild it"
    nm -g "$3" | awk 'NF >= 2 && $(NF-1) ~ /^[TW]$/ && $NF ~ /FunctionRegistrySnapshot.*capture/ { found=1 } END { exit !found }' || \
      die "Archive does not define the owned full registry snapshot; rebuild it"
    nm -g "$3" | awk 'NF >= 2 && $(NF-1) ~ /^[TW]$/ && $NF ~ /ownsMemoryPool/ { found=1 } END { exit !found }' || \
      die "Archive does not define Vector pool ownership; rebuild it"
    printf '%s\n%s\n%s\n%s\n%s\n' "$proof_version" "$pinned_commit" "$patch_digest" "$input_digest" "$archive_digest" \
      > "$3.pangrova-provenance"
    ;;
  *) die "Unknown patch operation: $action" ;;
esac
