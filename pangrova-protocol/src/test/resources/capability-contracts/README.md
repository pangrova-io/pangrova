# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-16 from the matching packaged Release
Native library after the Iceberg `$manifests` typed split changed the canonical
protocol descriptor. The cache-disabled function catalogue was captured
separately from the rebuilt `pangrova_function_export --catalogue` executable
(SHA-256 `c9d48c04d2b2573c2d8fe5f69e66e58410321238ded75f8c6ac437c52a9b5d71`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `7e4925c45904cb956e842244faaac6c127dbf3a19e009888346fde65d03ad3c8`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `85db890051b7cb92f5508c64cf2f93bc81e099deb4033059a5e92bdcb0254e40`.
- Inventory: 793,767 raw bytes, SHA-256
  `e054612c8200373ebc6103d3703f5c32cdec051779233516b80e96c8349d9c13`.

The E2E capture retained all 19 capabilities and 99 functions, including both
`file_from_url` and `file_from_bytes`. Text-format comparison against each
previous complete inventory changed only the canonical descriptor hash in the
protocol and function catalogue plus its derived manifest hash. No capability,
function, or relation advertisement changed.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
