# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete production Worker
advertisement, negotiated through JNI `GET_CAPABILITIES` and enriched by
`NativeCapabilityAdvertisement` with the configured model declarations. The
packaged Native runtime initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-21 after exposing the actual Native
`to_ieee754_32` and `to_ieee754_64` registrations through the shared SQL bindings.
The final complete inventory came from the rebuilt runtime containing the
canonical nested-vector name binding and microsecond TIME bridge repairs.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `b4055492101568e5d8e02f0be470d264e8e186d9a2cabdbc5c2fe6fd03bb339e`):

- Exported catalogue: 782,950 raw bytes, SHA-256
  `4d0723fc24aacc6b70ac7a2e4ae3d156ce4f853241e63e427b5ad27c96b71071`
  (9 capabilities and 87 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `75ff0461543a2ead0bd013851f5750a46d562200419d38e549c8a5d8460d4b6a`.
- Inventory: 795,569 raw bytes, SHA-256
  `2adbdb594720f509a75cf49e54649775100159f4c23b7323dab626cec2a81b97`.

The E2E capture retained all 19 capabilities and 103 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol owner module passed 448/448 tests after the UUID declaration repair,
before this final TIME recapture. The cache-disabled catalogue remains byte
identical to the earlier fixture. The affected actual-inventory contract class
passed 7/7 with the fresh complete publication. The final capture passed 1/1,
checking every Native field against the production Worker publication, then
asserting both configured model identities and exporting every model and
capability. Model instance signatures and binding hashes vary with the serving
instance, so these bytes are not compared as a global constant. Neither protocol
run had failures, errors or skips.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; the ordinary handshake
E2E now retains the permanent optional capture entry point below. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.

## Recapturing the complete JNI inventory

After rebuilding and packaging the matching Native library, install its Java
consumer chain and run the ordinary packaged handshake E2E. It validates the
complete raw JNI inventory and verifies that Worker publication preserves every
Native field while adding both deployed models, before optionally exporting the
unmodified publication. Raw JNI inventory alone does not contain these Worker
model declarations. The
output path must be absolute. Use the existing local model fixture prerequisites
and a FILE capability key so the deployed model and FILE descriptors are retained:

```bash
export PANGROVA_E2E_FILE_CAPABILITY_KEY="$(openssl rand -base64 32)"
mvn -q -pl pangrova-testing/pangrova-test-e2e -Pfull-regression \
  -Dtest=NativeCapabilityHandshakeE2ETest \
  -Dpangrova.e2e.ai.image-id="${PANGROVA_E2E_AI_IMAGE_ID:?set the verified local model image digest}" \
  -Dpangrova.e2e.native-inventory-output="$PWD/pangrova-protocol/src/test/resources/capability-contracts/actual-native-capability-inventory.bin" \
  test
```

The separate cache-disabled fixture is still produced by the matching
`pangrova_function_export --catalogue`. Record the two actual hashes and counts,
then run the affected protocol contracts; an export is not a test result.
