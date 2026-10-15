# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-16 from the matching packaged Release
Native library after the Paimon `$branches` typed split changed the canonical
protocol descriptor. The cache-disabled function catalogue was captured
separately from the rebuilt `pangrova_function_export --catalogue` executable
(SHA-256 `fd54430eb5d38b46eb8c814efac209b58a1607e8b4f478b143853a40ee2aa4e3`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `912c5db14526e2fc688dd3d2825ee213171d1db42043c6c0fd1e16be99281752`.

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `eb5cf5d35d2dc9099c24795b226f9d55aa65ed5c5d3312b3176e49587badd4d1`.
- Inventory: 793,767 raw bytes, SHA-256
  `90a04f02b33fb98f716d197fe7d9eb956c275bcef838c2408815d45a6a46d088`.

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
