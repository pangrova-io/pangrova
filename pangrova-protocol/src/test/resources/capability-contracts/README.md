# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after the packaged Worker Native runtime
initializes with the immutable artifact cache enabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was recaptured on 2026-10-14 from the matching packaged Release
Native library after the canonical v2 task statistics contract changed. The
cache-disabled function catalogue was captured separately from the current
Native exporter (SHA-256 `e7d999733e4e2c1ded8159e1b443ee56c5cfca66e88c53a8dc61e2a142ca5d25`).
The complete cache-enabled inventory came from actual E2E JNI startup:

- Native library SHA-256: `e4bd09d6a14f1fd371e40c93a3cf31f8ff495d4daac09d154a774c7c8069313e`.
- Inventory SHA-256: `949bb49aa127f8cda756442e3a313161de95d3b5934110b1acc97837d38bb0c8`.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. This capture used the E2E
harness's packaged JNI library and runtime initialization; its temporary capture
test was removed afterward. The fixture qualifies inventory validation against
this library. It does not qualify artifact execution, model invocation, or
end-to-end SQL.
