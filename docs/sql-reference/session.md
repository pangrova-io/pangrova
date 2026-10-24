---
title: "Session"
description: "Current SET behavior and request-scoped session context"
---

# Session

Query planning and connector access consume the session context supplied by the client.
The context can include `catalog`, `schema`, `timezone`, and `locale`.

## Current SET Behavior

The parser recognizes `SET property_name = value`. The default Coordinator registers
its handler, but `DdlExecutor.executeSetSession()` only logs the assignment and returns
an empty result. **It does not change server session state or the context of later queries.**

```sql
-- Recognized syntax; the default server does not persist this assignment.
SET catalog = 'iceberg_catalog';
```

Do not rely on a successful `SET` response to change table resolution or time-zone
behavior. Use fully qualified table names and supported client connection settings
or query-request session properties instead. A client may manage its own defaults;
that is separate from the server handler.

## Supplying Context

See the [REST API](../getting-started/clients/rest-api.md) for query request properties
and the [JDBC driver](../getting-started/clients/jdbc.md) for connection configuration.
Only properties consumed by the relevant planner or connector have an effect; a
property being passed through is not evidence that it is implemented.

Source evidence: [DDL executor](https://github.com/pangrova/pangrova/blob/main/pangrova-coordinator/src/main/java/org/pangrova/coordinator/query/statement/DdlExecutor.java).
See [Current Capability Status](../getting-started/capability-status.md).
