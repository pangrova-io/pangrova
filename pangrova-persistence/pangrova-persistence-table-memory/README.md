# Table Persistence Memory

Four explicit, instance-local stores implement the Table Persistence API: table
control and observations, maintenance policy/history, statistics, and governance
bindings. No store creates a global registry or a database connection. State is
lost on process restart and does not provide inter-process authority.

Control names use structured TableRef keys. Binding upserts preserve the winning
creation identity and return the row established by the current operation.
Maintenance defaults remain in Table Core, not in the persistence provider.

Table Core consumes the contract; Metadata/Server assembly selects implementations.
This artifact replaces the last production contents of the retired generic
`pangrova-persistence-memory` artifact. Query and Coordination have separate providers.
