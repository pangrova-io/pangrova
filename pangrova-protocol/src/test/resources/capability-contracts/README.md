# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-18 from the matching packaged Release
Native library after the typed COPY source-read task changed the canonical
protocol descriptor. The cache-disabled function catalogue was captured
separately from the rebuilt `pangrova_function_export --catalogue` executable
(SHA-256 `5af168fb2bc097b0d4d21975aaaaa0ede26267986aac7f45d7b26ad7539facde`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `418fb13d777f9ec53c54757513e0f3988f9a8053ef2b45efaa6b0454cbb4b57e`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `3ab5f9e19368ec5e6ddaa827c7aa7d0b52f24c7c6d89deae4f76613fc991e644`.
- Inventory: 793,767 raw bytes, SHA-256
  `c3a93626932fc9b5ba23a318a7ad8f8f247375766710479642eea992d1b66405`.

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
