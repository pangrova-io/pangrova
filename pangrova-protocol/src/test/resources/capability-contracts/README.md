# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-21 after exposing the actual Native
`min_by` and `max_by` aggregate registrations through the shared SQL bindings.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `4227de4a4872b45df612ff78412b76a27e3a4ac4afb2fa075428824571cd4bff`):

- Exported catalogue: 782,380 raw bytes, SHA-256
  `15e9ab95b35a2465b02d8e85598135f7c2b2b953383c01db843e5e273b6dbb06`
  (9 capabilities and 85 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `3a20a5e738efde6f5ec27c049277facf4b7e8ca6113ffb14939324c2601a7859`.
- Inventory: 794,999 raw bytes, SHA-256
  `774a8d069467ad67017f086c2569be31fef0285f0d7c7fa121c9beecb3f6f42e`.

The E2E capture retained all 19 capabilities and 101 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol owner module passed 447/447 tests with these fixtures, with no failures,
errors or skipped tests. The separate real-inventory capture test passed 1/1.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
