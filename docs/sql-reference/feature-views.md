---
title: "Feature Views"
description: "Feature View compilation, snapshot export, and security boundaries"
---

# Feature View compilation and snapshot export

Feature View definitions accept logical `source_query`, an exact
`catalog.schema.table` source, entity keys, a TIMESTAMP event column, output
schema, transform version, freshness target and lineage. Client execution plans
(`source_plan_base64`, plan hashes or other executable payloads) are rejected.

CREATE and every MATERIALIZE compile the logical SQL through the same
authenticated, query-scoped SQL compiler as ordinary queries. The source catalog
and schema, not the Feature View's namespace, bind relative source names. Actual
compiled output names/types must match the declared schema, entity keys and event
column. Stored server plans are audit evidence only and are never replayed.

MATERIALIZE accepts one canonical positive numeric snapshot. Historical connector
metadata must match the actual server-compiled scan schema. Lance records each
published version's typed handle and actual manifest SHA in the catalog CAS
transaction, then uses Native exact-version checkout to verify physical schema
and manifest SHA before exposing historical metadata or splits. Missing historical
publication evidence fails closed; current schema is not backfilled as history.

Export generations include the source snapshot, current principal, secured plan
digest, exact policy observation digest, and stage graph identity (terminal
purpose and partition cohorts). Durable dispatch, renewal and final
publication reauthorize the subject against the current persistent tenant,
membership, roles, grants and observed policies. A changed policy requires a fresh
MATERIALIZE. Rows and artifact bytes remain in the Native Worker data plane;
Coordinator publication handles only checksummed immutable object metadata.

Single-source definitions may include aggregates, windows, sorting and limits.
Their protected compiler-owned export DAG is frozen with exact stage parallelism,
globally unique page ordinals, original pinned source allocations and complete
producer dependencies. Only the final relation stages acquire a TABULAR_EXPORT
sink. Such jobs require the configured governed external shuffle service; they
are never run independently as one full SQL plan per split. Joins still require
the forthcoming explicit per-source snapshot map, and model/retrieval stages
require observed frozen model bindings before admission.
Zero-result filters still produce a schema-bearing stream and a published
zero-row manifest; execution failure is not an empty result.

Publication reuses the stage graph already verified by the current authorization
check in the publication transaction. After locking the job, the Coordinator
reads completed page allocations and their output objects together. Each terminal
partition must have its complete output roles, each compute partition must have
completed without publishing objects, and the page count must match the job.
Dispatch and recovery validate the exact split payloads and producer dependencies
when they use them. Publication uses the resulting fenced page states and outputs.

TABULAR_EXPORT has one relation-output contract: `RELATIONAL_PARTITION` pages
publish `TABULAR_DATA` in exact `WORK_UNIT` scope, without physical row locators
or physical-source output coverage. Even an empty input has a real terminal Native
task. PostgreSQL stores bounded canonical stage graph bytes and their SHA-256,
plus every source allocation and complete producer dependency cohort. The V43
migration refuses nonempty old flat dispatch specifications with an explicit
rebuild requirement and leaves their jobs and bytes intact; it never reconstructs
a graph from unauthoritative legacy payloads or deletes them automatically.

Every durable job has one persisted exact tenant/job-to-execution routing
association. The routing hash is not authorization. Tasks carry the actual
backend-issued Coordinator owner, process incarnation and execution token;
page-attempt fences never substitute for that identity. Claim, renewal,
completion and publication hold the same PostgreSQL execution-anchor row against
takeover in the metadata transaction, after current authorization and before
job/descriptor locks. Old Native owner evidence cannot be upgraded by copying a
new page fence. Query HA and maintenance-only publication share the Bootstrap's
backend/process binding; shutdown stops renewal and yields unfinished executions.
V46 refuses pre-owner artifact jobs transactionally with an explicit rebuild
requirement and preserves their recoverable metadata. Background recovery uses
the exact routing association and never releases active jobs merely because they
are absent from the user-query state manager.

External exchanges have separate immutable allocation and finalized-data evidence.
The allocation records exact graph/stage/snapshot identity, producer cohort and
partition endpoints, without access capabilities or provider credentials. The
ordinary shuffle lifecycle can restore that allocation and renew short-lived
grants at the actual service. Its actual finalize response bytes and checksum are
bound to the original allocation and the current backend owner; metadata page
success is not a data-durability proof. Consumer page claims validate this evidence
under the same current-security/execution-anchor transaction and remain blocked
until every incoming exchange is finalized. V47 refuses existing jobs with an
explicit rebuild requirement and preserves their original jobs and owner routes.
The ordinary DAG/Leaf now has typed current-PG durable control and the same
canonical task factory and HTTP dispatch boundary as live queries. Recovery uses
original Native winner bytes and their original owners/epochs, not reconstructed
Worker results. A failed pass retains external allocations; consumer claims still
require the real finalized-data barrier. End-to-end global cross-Worker result
acceptance is not yet verified; metadata/planner checks are not a substitute for
that gate, and a deployment without actual external shuffle cannot execute global
jobs.

The durable graph uses format 3. It hashes canonical operator bindings, indexed
logical result types, exact published input pointers, observed model identities,
and the planner's proof that eligible HASH inputs can be coalesced.
Old envelopes are explicitly rejected and require secure resubmission; their
stored jobs and allocations are not removed. Replay never discovers a new model
version or publication. A partition claim loads its exact immutable publication
objects and checks canonical source split digests on the claim connection. The
remaining task-local materialization is pure metadata computation and does not
borrow another database connection, perform network calls, or hold data rows.
Eligible compute stages freeze both their input ranges and physical producer
count before dispatch. A new owner reuses that decision during recovery.

Global relational export must reuse the generic stage DAG and durable artifact
barrier: pin each authorized source, schedule map/exchange/global relational
stages, and attach the existing TABULAR_EXPORT sink only to the final output
partitions. Publication must depend on every final partition's winning fenced
attempt, not on one arbitrary source split. Point-in-time multi-source features
add entity/event-time join semantics and independently pinned source watermarks;
they are not implemented by this single-source path. Online serving and training
dataset export require their own end-to-end acceptance evidence.
