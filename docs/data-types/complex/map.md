---
title: "MAP"
description: "Key-value mapping type"
---

# MAP


| Property | Value |
|----------|-------|
| Storage | Key-value mapping |
| Key type | Must be a primitive type |
| Value type | Any supported type |

**Declaration:**

```sql
CREATE TABLE configs (id BIGINT, settings MAP(VARCHAR, VARCHAR));
```

**Construction:**

```sql
-- Map literal
SELECT MAP['host', 'localhost', 'port', '8080'] AS config;
```

**Access:**

```sql
-- Access by key
SELECT settings['host'] AS host_value FROM configs;
```
