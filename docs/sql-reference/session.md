---
title: "Session"
description: "Current SET behavior and request-scoped session context"
---

# Session

Query planning and connector access consume the session context supplied by the client.
The context can include `catalog`, `schema`, `timezone`, and `locale`.

## Current SET Behavior

The parser recognizes `SET property_name = value`. Executing this statement returns
`PARSER_UNSUPPORTED_FEATURE`. Supply supported settings in query requests or client
connection configuration. `USE BRANCH` has a separate connection-scoped selection
protocol. See [versioning](versioning.md).

## Supplying Context

See the [REST API](../getting-started/clients/rest-api.md) for query request properties
and the [JDBC driver](../getting-started/clients/jdbc.md) for connection configuration.
Only properties consumed by the relevant planner or connector have an effect; a
property being passed through is not evidence that it is implemented.

See [Current Capability Status](../getting-started/capability-status.md).
