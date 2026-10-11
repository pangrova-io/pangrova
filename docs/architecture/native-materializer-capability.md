# Native materializer capability admission

Generated-values overlay preserves the input row set and appends materialized
values. Native publishes it only when the immutable artifact cache is ready.
Its typed relation uses fill-key and object-store-read grants and rejects
retrieval candidate grants. Java capability validation therefore distinguishes
materializers from retrieval sources, retrieval merges, and rerank operators.

Materializers require positive input/output byte budgets and the canonical typed
relation envelope. They must not advertise candidate-row or candidate-byte grants.
Retrieval capability candidate-row budgets remain mandatory and must fit positive
Java admission budgets. Unknown capability kinds are rejected. The positive
Protocol test retains the complete actual cache-enabled JNI inventory; negative
tests mutate that same inventory while retaining capability identity.

This corrects cache-enabled Worker capability admission. A matching Release
JNI bootstrap/close component captured the complete inventory with a real
immutable artifact cache enabled and verified allocation-ledger drain. The probe
released its own Node quota separately before checking the Root ledger. Artifact
execution, model invocation, and end-to-end SQL require their affected integration
chains; this admission correction does not establish those qualifications.
