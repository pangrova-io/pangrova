# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-21 after exposing the actual Native
`to_ieee754_32` and `to_ieee754_64` registrations through the shared SQL bindings.
The final complete inventory came from the packaged runtime containing the
canonical nested-vector name binding repair.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `302358d1a1e66ad159ebd150cf3cd127890a3f42154badcc49b230e499bc2125`):

- Exported catalogue: 782,950 raw bytes, SHA-256
  `4d0723fc24aacc6b70ac7a2e4ae3d156ce4f853241e63e427b5ad27c96b71071`
  (9 capabilities and 87 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `af3b151da8750d14e0c1c53f451719a453242cf100158d69b19912b3bdbdbaf5`.
- Inventory: 795,569 raw bytes, SHA-256
  `b344d0fbaa0727ae6a04a54a8e754c568067237f5106d52f7a13460e284eb7fb`.

The E2E capture retained all 19 capabilities and 103 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol owner module passed 447/447 tests with the initial two-function
recapture. The final inventory changes only the two model instance signatures
and their binding contract hashes; these correctly vary with the serving
instance. Only the affected actual-inventory contract class was rerun, passing
7/7 with the final full inventory. The final capture passed 1/1, comparing the
complete stable capability/function/catalogue publication without discarding
any models or capabilities. Serving-instance bytes are not compared as a global
constant. Neither protocol run had failures, errors or skips.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.

## Recapturing the complete JNI inventory

After rebuilding and packaging the matching Native library, install its Java
consumer chain and run the ordinary packaged handshake E2E. It validates the
complete inventory before optionally exporting the unmodified message. The
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
