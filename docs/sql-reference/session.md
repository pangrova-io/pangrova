---
title: "Session"
description: "SET session property statements"
---

# Session

Session-level statements that adjust the behavior of subsequent queries in the same session.


## Session Management

### SET

```sql
SET property_name = value;
```

**Example:**

```sql
-- Set the default catalog for unqualified table names
SET catalog = 'iceberg_catalog';

-- Set the default schema
SET schema = 'analytics';

-- Set the session time zone
SET timezone = 'UTC';

-- Set the session locale
SET locale = 'en_US';
```

Recognized session properties include `catalog`, `schema`, `timezone`, and
`locale`; additional properties are passed through to connectors.
