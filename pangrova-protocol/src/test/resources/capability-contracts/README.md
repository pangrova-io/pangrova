# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-20 from the matching packaged Release
Native library after stable partition field IDs and canonical names changed the
canonical protocol descriptor.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `40a63eda43c930dad194f33ce0e40783a18cf357ee69e08c888b74cb8c42bde5`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `2b69922e9cb45c5883a7f13102bfd396edab39af704db031dede8308d1426d11`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `8172ad7e5f87164566db5403444727d61d846b9a427cfc4ac124a9507733228f`.
- Inventory: 794,415 raw bytes, SHA-256
  `58f57c12506e1dfc5e6bc678d92949fef3db3fe5c55a5aa75d7531e2e524f29f`.

The E2E capture retained all 19 capabilities and 99 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol module passed 447 tests with the recaptured fixtures, with no failures
or skipped tests.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
