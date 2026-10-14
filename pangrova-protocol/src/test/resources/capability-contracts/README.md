# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-14 from the matching packaged Release
Native library after the canonical v2 result-server bind-address contract changed. The
cache-disabled function catalogue was captured separately from the current
Native exporter (SHA-256 `c615388f67918ee4d44b0d5b56452ab857773b9bec8e34ae0f866df5a788602a`).
The complete cache-enabled inventory came from actual E2E JNI startup:

- Native library SHA-256: `a769ab2dccd8be95dd1def4e5a3e31c0360ea9b550292479d98c1380abdcae23`.
- Inventory SHA-256: `a3511e913a26311a87cf240f29f098ada8ba4a103cab443e0e718d0b76d0f4fd`.

The E2E capture enabled the real governed FILE callback deployment before Native
startup. It retained 19 capabilities and 99 functions; text-format comparison
against the previous inventory changed only descriptor and derived manifest hashes.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
