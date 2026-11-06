# Indexing Planner Adapter

This artifact owns the existing Calcite vector-search recognition path and its
Native retrieval planning values. It depends on Planner Core, Indexing API and
Protocol. Indexing Core and persistence implementations are not dependencies.

The production caller is Coordinator's `DispatchContextEnricher`: it supplies the
query's tenant-scoped index lookup to `VectorSearchRewriter`, retains the resulting
`VectorSearchNode`, and binds source/index snapshot observations before dispatch.
`RetrievalIntentCollector` and the DAG binding path still reside in Coordinator;
this extraction does not complete their domain separation.

The adapter has no database, executor, plugin registry or closeable resources. It
borrows the caller's lookup for one planning operation. The node preserves its
existing array/list ownership and exact capability identity checks. Recognition,
metric checks, recall estimation, supported physical input, filtered DiskANN
eligibility and search-budget bounds are unchanged by this move.

Generic `IndexHint` belongs to Indexing API's `api.pruning` package.
`IndexPredicateAnalyzer` belongs to Indexing Core's `core.pruning` package.
The unused alternative TopK recognizer/router/hybrid planner and `QueryPlan`
wrapper have been removed. Their former unit tests were not evidence of an
available SQL path; in particular, this change does not advertise support for
`NO_VECTOR_INDEX` or enable a different ANN/filter policy.

Validation is recorded in
[the modularization ledger](../../docs/internal/project-structure-refactoring/domain-modularization-progress.md).
A passing SQL-result test may use the normal scan fallback; only tests that inspect
actual bindings/runtime evidence can establish that a Native artifact was used.
