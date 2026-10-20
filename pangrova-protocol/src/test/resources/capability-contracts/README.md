# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-21 from the matching packaged Release
Native library after the ICEBERG_REFS split kind changed the canonical protocol
descriptor. A final collection followed the SDK reference-type parsing update.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `736a52f8e68fe17cf97c084bda46ca444df3717ee0a0e4ae7fd38efa07c1165b`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `8f239544177fa447fa7eda29a896fa382c4f32ac9f62f76fe112eb3010394286`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `d48301cd14a6291a8439b2bcb0094021bdc9ae451e72a2b4ed4e7c54e8d32893`.
- Inventory: 794,415 raw bytes, SHA-256
  `5d0b9be8b61c0eade4b2245c57aaec28d30793d7dbc09a7af1a51da322252da5`.

The E2E capture retained all 19 capabilities and 99 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Both binary files were parsed with the generated current
protocol classes to verify their capability, function and model counts. The
protocol module passed 447 tests with the first recaptured fixtures, with no
failures or skipped tests. The final actual-inventory collection changes serving
fingerprints; only the affected actual-inventory contract class was rerun,
passing 7/7 checks. The exported catalogue bytes were unchanged.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
