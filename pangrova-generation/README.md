# Generation

Generation owns immutable artifact job identities, work-page descriptions, stage graphs,
and publication values. It does not own Dynamic Table or Materialized View lifecycles.

| Artifact | Responsibility |
| --- | --- |
| `pangrova-generation-api` | Immutable dependency, execution, identity, job, page, graph, exchange, output and publication contracts; value validation and defensive copies |
| `pangrova-generation-codec` | Bounded canonical graph/exchange encoding, source split decoding and the supported wire-format role layouts |
| `pangrova-generation-core` | Submission construction, frozen bindings, exact partition/resource preparation, published-input validation and publication routing |

API depends directly on Common, Query API (the existing execution anchor) and Security API
(the existing immutable authorization observation). It does not load Protocol, Core, Codec,
Persistence ports, PostgreSQL, Calcite, Arrow or provider SDKs. The real minimal-classpath
contract test walks every compiled API class, including nested records and generic signatures.
There is no empty `.spi` package: this extraction moves value contracts; actual persistence
extension ports remain in Persistence until their separate domain split.

`GenerationOutputFormat` and `GenerationOutputRole` describe an immutable layout.
`generation.codec.format.BuiltInGenerationOutputFormats` selects the currently supported
wire layouts and rejects unknown format/artifact combinations. It is not a provider registry
or a declaration that arbitrary execution implementations are available. PostgreSQL admission
and publication validation share those layouts without depending on Generation Core.

Persistence remains the authority for jobs, leases, execution fences and publication records.
An API value is a snapshot, not a new state-machine owner or an authorization credential.
Stores still verify current authority and exact attempts on the caller's transaction. Stage
and split checksum identities, immutable input bounds, empty-partition policy and canonical
encoding are unchanged by the package move.

Server assembly still owns provider selection and process resources. Generation API allocates
no connections, threads, native handles or clients. Completion byte arrays remain defensively
copied; encoded stage/input values do not transfer ownership of external data objects.

The existing exchange evidence still describes the current Pangrova remote shuffle allocation.
Extracting it into this API does not complete the planned provider-neutral Shuffle descriptor,
receipt or Celeborn integration. Coordinator dispatch, cross-domain workflows and the remaining
Generation persistence split continue under the approved modularization plan.
