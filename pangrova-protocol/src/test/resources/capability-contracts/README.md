# Actual Native capability inventory

`actual-native-capability-inventory.bin` is the complete, unmodified inventory
returned by JNI `GET_CAPABILITIES` after production runtime bootstrap with the
immutable artifact cache enabled. Function registration was disabled. It includes
the generated-values materializer, whose availability depends on the actual
immutable-cache readiness gate. It is distinct from the cache-disabled function
exporter catalogue used by function-contract tests.

The snapshot was captured on 2026-10-08 from the matching Release Native library:

- Native library SHA-256: `4ff5c4be7e5d24325a1b6a7ba5618c50e4720d0b680bb835c7cd0564fa7b119b`.
- Inventory SHA-256: `c6281068497cdda6c833cb47501d9dd227ccb03d82edd9a4108fe7bd142d8ace`.

Tests retain every actual capability and function in the positive round trip.
Negative inputs mutate the actual inventory while retaining capability identity.
No capability is removed to bypass startup validation. The producer used strict
Java compilation and `-Xcheck:jni`. Capture r6 retained the complete inventory and
verified real Java/Native allocation ledgers were zero after runtime shutdown. It
then released the probe's own node quota and verified the Root ledger was zero.
Earlier r4/r5 failures retained that node quota in Root; the recorded allocation
ledgers were already zero. No production allocator behavior was changed to make
this observation pass. This fixture qualifies inventory validation and this
matching library's bootstrap/close component only; it does not qualify artifact
execution, model invocation, or end-to-end SQL.
