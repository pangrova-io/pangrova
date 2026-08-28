---
title: "VECTOR"
description: "Fixed-dimension embedding vector type"
---

# VECTOR


| Property | Value |
|----------|-------|
| Storage | Fixed-dimension floating-point vector |
| Default element | `FLOAT` (32-bit) |
| Optional element | `DOUBLE` (64-bit) |
| Use case | Embedding vectors for similarity search |

**Declaration:**

```sql
CREATE TABLE articles (
    id BIGINT,
    content VARCHAR,
    embedding VECTOR(1024),         -- 1024-dim float vector
    precise_emb VECTOR(768, DOUBLE)  -- 768-dim double vector
);
```

**Construction:**

```sql
-- Vectors are constructed using ARRAY literal syntax
INSERT INTO articles (id, embedding) VALUES (1, ARRAY[0.1, 0.2, 0.3, ...]);
```

**Distance Functions:**

```sql
SELECT id, l2_distance(embedding, ARRAY[...]) AS dist
FROM articles
ORDER BY l2_distance(embedding, ARRAY[...]) ASC
LIMIT 10;
```

**Index:**

```sql
CREATE INDEX idx_emb ON articles(embedding)
    USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2');
```

**See Also:** [Vector Search](../../sql-reference/functions/vector-search.md), [AI Functions](../../sql-reference/functions/ai-functions.md)
