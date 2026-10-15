# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-16 from the matching packaged Release
Native library after the Iceberg `$files` typed split changed the canonical
protocol descriptor. The cache-disabled function catalogue was captured
separately from the rebuilt `pangrova_function_export --catalogue` executable
(SHA-256 `1ae8c8f7f7152b8cdf89055a27cb7e396c20eef03e475eea93a5f1919bfc373d`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `aad551fd052232e7a9ea32967ab2e35275e427f3c9dbad6a9a65a072053e162e`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `495ffe335405fbead2c124d4cce728440015dc07b0a4f7034ffa17be4dde8af6`.
- Inventory: 793,767 raw bytes, SHA-256
  `142db04be66d60da60c1e5bf648e2f6ef8bac40af8d5b531a5c69c52e4ef930b`.

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
