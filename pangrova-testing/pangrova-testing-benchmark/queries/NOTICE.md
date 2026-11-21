# TPC-derived workload provenance

TPC Benchmark, TPC-H, TPC-DS, QphH and QphDS are trademarks of the Transaction
Processing Performance Council. Original templates and schema definitions are
copyright TPC and their respective contributors. Source legal notices are retained
in `schemas/*-source.sql`; the source kits and their terms are linked in
`tools.lock.json`.

The checked-in engineering SQL was generated from checksum-pinned community
sources: TPC-H tool version 2.14.0 and TPC-DS tool version 2.10.0. These are not
current official TPC-H 3.0.1 / TPC-DS 4.0.0 tool releases. Every template's source
checksum, generator settings, executable-step checksums, SQL dialect adaptations
and outstanding semantic review are recorded in `workload.json`.

Current official specifications and tool distributions:
[TPC downloads](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
These derived workload results must not be published as certified TPC scores.
