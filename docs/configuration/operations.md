---
title: "Operations Configuration"
description: "Operations module flag and maintenance execution model"
---

# Operations

Day-2 operations configuration.


## Operations Configuration

The Coordinator reads this flag when installing its in-process recovery playbook.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `operations.enabled` | bool | `true` | Create the recovery playbook. Incidents without an installed action are escalated. |

The coordinator deployment role selects whether a node runs query workloads,
background maintenance tasks, or both:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.role` | string | `"COMBINED"` | `QUERY` (serve queries only), `MAINTENANCE` (background tasks only), or `COMBINED` (both) |

The playbook accepts explicit incidents from its caller. This distribution has no
automatic incident subscriber or installed action for worker loss, task timeout,
or out-of-memory events. The flag does not change SQL dispatch, enable HTTP
diagnostics, or start infrastructure automation.

---

## How Maintenance Runs

Maintenance is not driven by YAML schedules. It is executed on demand through
SQL `CALL` procedures and the maintenance coordinator role:

- **Table maintenance** — `CALL <catalog>.compact(...)`,
  `expire_snapshots(...)`, `remove_orphan_files(...)`, and the other
  connector procedures. See [Maintenance](../sql-reference/maintenance.md).
- **Warmup** — `CALL warmup_table(...)` preloads metadata and indexes for a
  table before it serves queries.
- **Rolling upgrade / cluster status** — operational scripts in `bin/`
  (`pangrova-rolling-upgrade.sh`, `pangrova-cluster-status.sh`,
  `pangrova-health-check.sh`).

**See Also:** [Maintenance](../sql-reference/maintenance.md), [Coordinator](coordinator.md)
