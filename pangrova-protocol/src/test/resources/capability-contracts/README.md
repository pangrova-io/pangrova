# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-19 from the matching packaged Release
Native library after `ICEBERG_ENTRIES` changed the canonical protocol descriptor.
The cache-disabled function catalogue was captured separately from the rebuilt
`pangrova_function_export --catalogue` executable
(SHA-256 `fef0d58fe75089d965841fc85fa8a491a64610f8b861815d163f1ca066de009f`):

- Exported catalogue: 781,796 raw bytes, SHA-256
  `38b3190b624b9916773de951cef99150068f8002ad88c5c125ae38201d02ae12`
  (9 capabilities and 83 functions).

The complete cache-enabled inventory came from actual E2E JNI startup with
`PANGROVA_E2E_FILE_CAPABILITY_KEY` set, which deployed the governed FILE
callbacks before Native catalogue publication:

- Native library SHA-256: `d879accaa07f2f8ddcf47dfa9c5f03032a4bc9bfab22760a05a3eb7a5ebb1d76`.
- Inventory: 794,415 raw bytes, SHA-256
  `2e82f87cfc66259ffe0a09887a650c8938680252fcc22c6eb6ef00065a0c641c`.

The E2E capture retained all 19 capabilities and 99 functions, including both
`file_from_url` and `file_from_bytes`, and advertised the two deployed local
model revisions. Wire-format comparison against the previous complete inventory
showed the canonical descriptor hash and derived catalogue manifest hash changed,
along with those two live model-availability records. No capability, function,
or relation advertisement changed. The cache-disabled catalogue differed only
in the descriptor and derived manifest hashes.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
