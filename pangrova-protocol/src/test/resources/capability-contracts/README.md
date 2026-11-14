# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete production Worker
advertisement, negotiated through JNI `GET_CAPABILITIES` and enriched by
`NativeCapabilityAdvertisement` with the configured model declarations. The
packaged Native runtime initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

## Current capture

On 2026-11-14 the macOS arm64 Worker handshake exported the current inventory
from the packaged JNI runtime with the immutable cache, governed FILE callbacks
and both configured models. The cache-disabled catalogue came from the
`pangrova_function_export --catalogue` binary built alongside that runtime.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Native library | 322876581 | `a99793594d40a57aa2a1c025bb3920596ae4d23d148d5530921aea1a99fc3e1a` |
| Worker inventory | 795600 | `b49a040ef5c0f3b3afbb03c27cc8b5661b0319b4a3eea9129dbb1d6b47c225db` |
| Function exporter catalogue | 782981 | `8ae14ab54c5af7af34b3946c0e335ac44e15e28d666ae57cd0afe4c20153952e` |

Both fixtures carry canonical descriptor SHA-256
`ab4d05e056dea402e400916ba7b05144e5573108dc5e3ffa272de70062b23ed0`,
including the Coordinator partition publication evidence in the external exchange binding.
The Worker advertised 19 capabilities, 103 functions and two deployed models.
The exporter reported nine capabilities, 87 functions and zero models.

## October capture

The complete cache-enabled snapshot was recaptured on **2026-10-25** through
`NativeCapabilityHandshakeE2ETest`, after the worker-incarnation protocol change.
The current-source Native target built successfully and the packaged library
matched the build artifact. The real PostgreSQL/Ollama/JNI handshake passed 1/1
with no skipped tests and retained all 19 capabilities, 103 functions (including
the governed FILE callbacks), and two configured models. October capture receipts:

- Native library SHA-256: `6dbd54c01694177de9347cd9482121a1d91af95cc7ef3bbcfcc4ea1310942f38`.
- Inventory: 795,600 bytes, SHA-256
  `cd658bdd93f7e0ce23ba964ef3e997c300f7dafedba9f02495cb89339118bf87`.
- Canonical descriptor SHA-256:
  `8e19c1d352460f0f950613fb744ac0228048a6656da001b945dde5568bd46244`.
- Prepared Ollama image:
  `sha256:07fd8f7607f360ed6c29d2d976daa7ebcbe200a63b81e9daecbf561866d1cb66`.

The E2E harness now wires the production controlled-maintenance executor to real
PostgreSQL policy and lease stores. Passing `null` to the mandatory executor had
prevented coordinator startup before the handshake could run. Neither the
ownership checks nor the capability validator were relaxed for this recapture.
The protocol owner suite then passed all 465 tests across 81 classes, with zero
failures, errors, or skips, including all seven actual-inventory contract cases.
This receipt qualifies handshake and publication, not SQL feature completeness
or the full production regression.

## Historical capture and function exporter

The preceding snapshot was captured on 2026-10-21 after exposing the actual Native
`to_ieee754_32` and `to_ieee754_64` registrations through the shared SQL bindings.
This historical complete inventory came from the rebuilt runtime containing the
canonical nested-vector name binding, microsecond TIME bridge and UUID
identity/unsigned-ordering repairs. Both catalogues retain all 22 physical
projections, including UUID as its distinct logical type over HUGEINT.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `88d049d5767d84dc11349e6a6afcf2724ca1b344c34e453dadb1d6dc8d5fa8a6`):

- Exported catalogue: 782,981 raw bytes, SHA-256
  `1c462e4c925da95480712065b6c90f8b2e69aecdbe95d7e9602e673fee225dc0`
  (9 capabilities and 87 functions).

That historical complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `8131c2beb2797ca186cd1114d81dd2a8e79cfb2439b72efb6196b9942524144d`.
- Inventory: 795,600 raw bytes, SHA-256
  `5dc30e6ed0a7c4c18a77841ce3ecfa8a23d2e5011a138c899f6f47cb790855d1`.

The E2E capture retained all 19 capabilities and 103 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol owner run covered 448 tests against the UUID fixtures. Its three
position-dependent declaration checks failed after adding the UUID projection;
they now locate projections by declared kind and all three passed on targeted
rerun. The rolling union covers all 448 unique cases without failures or skips;
static analysis and installation also passed. The complete UUID capture passed
1/1, checking every Native field against the production Worker publication, then
asserting both configured model identities and exporting every model and
capability. Its source run also exposed UUID SQL/parser and cold Arrow
materialization failures; handshake success does not qualify those SQL paths.
Model instance signatures and binding hashes vary with the serving
instance, so these bytes are not compared as a global constant. The fixture
update preserves the full production publication.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; the ordinary handshake
E2E now retains the permanent optional capture entry point below. The fixture qualifies inventory validation against
that captured library. Subsequent UUID CAST-length and Parquet-reader repairs have
separate matching-library runtime evidence; this historical binary is not a claim
that it was recaptured from every later build. It does not qualify artifact execution, model invocation, or
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
mvn -q -pl pangrova-testing/pangrova-testing-integration-tests -Pfull-regression \
  -Dtest=NativeCapabilityHandshakeE2ETest \
  -Dpangrova.e2e.ai.image-id="${PANGROVA_E2E_AI_IMAGE_ID:?set the verified local model image digest}" \
  -Dpangrova.e2e.native-inventory-output="$PWD/pangrova-protocol/src/test/resources/capability-contracts/actual-native-capability-inventory.bin" \
  test
```

The separate cache-disabled fixture is still produced by the matching
`pangrova_function_export --catalogue`. Record the two actual hashes and counts,
then run the affected protocol contracts; an export is not a test result.
