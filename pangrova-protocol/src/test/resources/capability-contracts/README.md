# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-15 from the matching packaged Release
Native library after the `GENERATE_SERIES` function declaration and Native
relation inventory changed. The cache-disabled function catalogue was captured
separately from the rebuilt `pangrova_function_export --catalogue` executable
(SHA-256 `a8e49ae138759d3a032863397af3dc9d2fa89b18138783b685d539c1c11cdca8`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `bc63582e20e4942a58f24f7af098d829a80c0ea91d46fd61a6c177264fde9713`.

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `f940e80f17f4c0e0df388a217750cdc7021541cd87a0bfc23bfb40e1bf711d3a`.
- Inventory: 793,767 raw bytes, SHA-256
  `b71987d6e748aa9a93e6cb91523f2bada176787ac38b2710ac3b8e0961c8f039`.

The E2E capture retained all 19 capabilities and 99 functions, including both
`file_from_url` and `file_from_bytes`. Text-format comparison against the
previous inventory changed only the binding-declaration and derived manifest
hashes and removed the unsupported `ExtensionMultiRel` advertisement.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
