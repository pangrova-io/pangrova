# Table

`pangrova-table` is an aggregator. The former code artifact is replaced by two real leaves:

| Artifact | Responsibility |
| --- | --- |
| `pangrova-table` | Table identity, ownership, version observations, rename intent, maintenance/statistics values and the policy authority read port |
| `pangrova-table` | Existing table control, policy binding, maintenance and statistics orchestration; ownership-dependent maintenance defaults |

The API uses Common and Connector API (for immutable table statistics). It does not depend on
Persistence ports, Core, Metadata, Protocol, a database, an SDK or process assembly. There is
no empty SPI package: the existing `api.policy.TablePolicyAuthority` is a narrow calling
port for policy consumers, not a provider registry.

Core consumes persistence contracts and receives stores through constructors. Store facts,
optimistic concurrency, version-observation atomicity and invalidation remain owned by the
existing Persistence implementations. Core does not construct a PostgreSQL or Memory backend.
Memory is used only by its test fixtures. Metadata/Server assembly still selects providers.

`TableMaintenancePolicy` owns immutable values, bounds, operation aliases and copy-with methods.
The ownership-dependent default policy is selected by Core's `TableMaintenanceDefaults`;
LINKED defaults remain empty and guarded, and MANAGED defaults retain their existing limits.
Moving this selection does not grant new maintenance capabilities or change connector support.

`TableControlService` currently still combines ownership admission, registration, version
observation and rename orchestration. Its decomposition into the approved capability boundaries
remains separate work. Existing shadow/enforcement settings and explicit legacy admission paths
have not been changed by this artifact extraction. This is not final production acceptance.
No historical data migration, dual-write path, compatibility forwarding class or new backend
is introduced. Physical identity, catalog incarnation/revision, rename reservation, current
permissions and atomic store operations retain their previous checks.
