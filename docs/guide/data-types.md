Pangrova > Guide > Data Types

# Data Types

Complete reference for all data types supported by Pangrova. Each type maps to the internal Substrait type system for distributed execution.

---

## 1. Numeric Types

### BOOLEAN

| Property | Value |
|----------|-------|
| Storage | 1 bit |
| Values | `TRUE`, `FALSE`, `NULL` |

```sql
CREATE TABLE flags (id BIGINT, active BOOLEAN);
INSERT INTO flags VALUES (1, TRUE), (2, FALSE);
```

---

### TINYINT

| Property | Value |
|----------|-------|
| Storage | 8-bit signed integer |
| Range | -128 to 127 |

```sql
CREATE TABLE sensors (id BIGINT, reading TINYINT);
```

---

### SMALLINT

| Property | Value |
|----------|-------|
| Storage | 16-bit signed integer |
| Range | -32,768 to 32,767 |

```sql
CREATE TABLE events (id BIGINT, code SMALLINT);
```

---

### INT / INTEGER

| Property | Value |
|----------|-------|
| Storage | 32-bit signed integer |
| Range | -2,147,483,648 to 2,147,483,647 |
| Aliases | `INT`, `INTEGER` |

```sql
CREATE TABLE users (id INT, age INTEGER);
```

---

### BIGINT

| Property | Value |
|----------|-------|
| Storage | 64-bit signed integer |
| Range | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |

```sql
CREATE TABLE orders (order_id BIGINT, timestamp_ms BIGINT);
```

---

### FLOAT

| Property | Value |
|----------|-------|
| Storage | 32-bit IEEE 754 floating point |
| Precision | ~7 decimal digits |

```sql
CREATE TABLE measurements (id BIGINT, temperature FLOAT);
```

---

### DOUBLE

| Property | Value |
|----------|-------|
| Storage | 64-bit IEEE 754 floating point |
| Precision | ~15 decimal digits |
| Alias | `DOUBLE PRECISION` |

```sql
CREATE TABLE coordinates (id BIGINT, latitude DOUBLE, longitude DOUBLE PRECISION);
```

---

### DECIMAL(p, s)

| Property | Value |
|----------|-------|
| Storage | Arbitrary-precision decimal |
| Parameters | `p` = precision (total digits), `s` = scale (digits after decimal) |
| Alias | `NUMERIC(p, s)` |

```sql
CREATE TABLE transactions (id BIGINT, amount DECIMAL(10, 2), rate NUMERIC(5, 4));
INSERT INTO transactions VALUES (1, 1234.56, 0.0375);
```

---

## 2. String Types

### VARCHAR / VARCHAR(n)

| Property | Value |
|----------|-------|
| Storage | Variable-length UTF-8 string |
| Max length | Optional constraint via `VARCHAR(n)` |
| Alias | `STRING` |

```sql
CREATE TABLE profiles (
    id BIGINT,
    name VARCHAR,          -- Unbounded
    email VARCHAR(255),    -- Max 255 characters
    bio STRING             -- Alias for VARCHAR
);
```

**Operators:**
- Concatenation: `||` or `CONCAT(a, b)`
- Pattern matching: `LIKE`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`

---

## 3. Date/Time Types

### DATE

| Property | Value |
|----------|-------|
| Storage | Calendar date (year, month, day) |
| Format | `YYYY-MM-DD` |

```sql
SELECT DATE '2024-01-15' AS d;
INSERT INTO events (event_date) VALUES (DATE '2024-06-01');
```

---

### TIME

| Property | Value |
|----------|-------|
| Storage | Time of day without timezone |
| Format | `HH:MM:SS` |

---

### TIMESTAMP

| Property | Value |
|----------|-------|
| Storage | Timestamp with microsecond precision (without timezone) |
| Format | `YYYY-MM-DD HH:MM:SS.ffffff` |

```sql
SELECT TIMESTAMP '2024-01-15 10:30:00' AS ts;
INSERT INTO logs (created_at) VALUES (TIMESTAMP '2024-06-01 12:00:00.123456');
```

---

### TIMESTAMP WITH TIME ZONE

| Property | Value |
|----------|-------|
| Storage | Timestamp with microsecond precision and timezone |
| Internal type | `TIMESTAMP_TZ` |

```sql
SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00+08:00' AS ts_tz;
```

---

### TIMESTAMP_NS

| Property | Value |
|----------|-------|
| Storage | Timestamp with nanosecond precision (without timezone) |
| Use case | High-precision event logging, IoT sensor data |

```sql
CREATE TABLE high_freq_events (id BIGINT, event_time TIMESTAMP_NS);
```

---

### INTERVAL

| Property | Value |
|----------|-------|
| Variants | `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` |
| Use case | Date/time arithmetic |

```sql
-- Year-to-month interval
SELECT INTERVAL '1-6' YEAR TO MONTH;

-- Day-to-second interval
SELECT INTERVAL '3 12:30:00' DAY TO SECOND;

-- Date arithmetic
SELECT event_time + INTERVAL '7' DAY AS next_week FROM events;
SELECT order_date - INTERVAL '1' MONTH AS last_month FROM orders;
```

---

## 4. Complex Types

### ARRAY(element_type)

| Property | Value |
|----------|-------|
| Storage | Ordered collection of elements |
| Element type | Any supported type |
| Nesting | Supported (e.g., `ARRAY(ARRAY(INT))`) |

**Declaration:**

```sql
CREATE TABLE tags (id BIGINT, labels ARRAY(VARCHAR));
```

**Construction:**

```sql
-- Array literal
SELECT ARRAY['red', 'green', 'blue'] AS colors;

-- Insert array values
INSERT INTO tags VALUES (1, ARRAY['urgent', 'bug']);
```

**Access:**

```sql
-- Access by index (1-based in SQL context)
SELECT labels[1] AS first_label FROM tags;
```

**EXPLODE (table function):**

```sql
-- Unnest array to rows
SELECT t.id, label
FROM tags t, LATERAL TABLE(EXPLODE(t.labels)) AS e(label);

-- With position
SELECT t.id, pos, label
FROM tags t, LATERAL TABLE(POSEXPLODE(t.labels)) AS e(pos, label);
```

---

### MAP(key_type, value_type)

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

---

### STRUCT(field_name type, ...)

| Property | Value |
|----------|-------|
| Storage | Named field composite type |
| Field access | Dot notation |
| Aliases | `ROW` constructor |

**Declaration:**

```sql
CREATE TABLE contacts (
    id BIGINT,
    address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)
);
```

**Construction:**

```sql
-- STRUCT constructor with named fields
SELECT STRUCT(name => 'Alice', age => 30) AS person;

-- ROW constructor (positional)
SELECT ROW('123 Main St', 'Seattle', '98101') AS addr;
```

**Access:**

```sql
-- Dot notation for field access
SELECT address.city, address.zip FROM contacts;
```

---

## 5. AI-Native Types

### VECTOR(dim [, element_type])

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

**See Also:** [Vector Search](vector-search.md), [AI Functions](ai-functions.md)

---

### FILE

| Property | Value |
|----------|-------|
| Storage | STRUCT (claim-check pattern) |
| Internal | `ROW<uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP>` |
| Use case | Multimodal AI function input (images, documents, audio) |

**Declaration:**

```sql
CREATE TABLE documents (id BIGINT, attachment FILE);
```

**Construction:**

```sql
-- From URL
INSERT INTO documents VALUES (1, FILE_FROM_URL('s3://bucket/doc.pdf'));

-- From raw bytes
INSERT INTO documents VALUES (2, FILE_FROM_BYTES(X'89504E47...', 'image/png'));
```

**Usage with AI functions:**

```sql
SELECT AI_DESCRIBE_IMAGE('gpt-4o', attachment) AS description FROM documents;
SELECT AI_OCR(attachment) AS text FROM documents;
```

**Supported URI schemes:** `s3`, `oss`, `gs`, `gcs`, `adls`, `abfs`, `abfss`, `hdfs`, `http`, `https`, `blob`

**See Also:** [AI Functions — File Helpers](ai-functions.md#3-file-helper-functions)

---

## 6. Semi-Structured Types

### VARIANT

| Property | Value |
|----------|-------|
| Storage | Semi-structured JSON with shredded columnar encoding |
| Insert format | JSON string literal |
| Query access | `variant_get(column, path)` function |

**Declaration:**

```sql
CREATE TABLE events (
    id BIGINT,
    payload VARIANT
) TBLPROPERTIES ('format-version' = '3');
```

**Insert:**

```sql
INSERT INTO events (id, payload) VALUES
    (1, '{"name": "alice", "age": 30}'),
    (2, '{"name": "bob", "address": {"city": "seattle"}}'),
    (3, '{"items": [{"name": "widget", "qty": 5}]}');
```

**Path Expressions:**

```sql
-- Dot notation: top-level field
SELECT variant_get(payload, 'name') AS name FROM events;

-- Bracket notation: special characters in key
SELECT variant_get(payload, '[''user-name'']') AS uname FROM events;

-- Nested dot path
SELECT variant_get(payload, 'address.city') AS city FROM events;

-- Array index access
SELECT variant_get(payload, 'items[0]') AS first_item FROM events;

-- Mixed nested path
SELECT variant_get(payload, 'items[0].name') AS item_name FROM events;
```

**Notes:**
- Returns NULL for missing paths or out-of-bounds array indices
- Supports both Iceberg and Paimon connectors
- Iceberg requires `format-version = '3'` for VARIANT columns
- Internally uses shredded columnar encoding for efficient selective access

---

## 7. Spatial Types

### GEOMETRY

| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) encoded |
| CRS | CRS:84 (WGS 84, lon/lat) by default |
| Use case | Planar (Cartesian) geospatial operations |

**Declaration:**

```sql
CREATE TABLE locations (
    id BIGINT,
    shape GEOMETRY
) TBLPROPERTIES ('format-version' = '3');
```

**Insert (WKB hex):**

```sql
-- POINT(1.0 2.0) as WKB little-endian
INSERT INTO locations VALUES (1, X'0101000000000000000000F03F0000000000000040');
```

---

### GEOGRAPHY

| Property | Value |
|----------|-------|
| Storage | Well-Known Binary (WKB) encoded |
| CRS | CRS:84 (WGS 84, spherical coordinates) |
| Use case | Geodesic (spherical) geospatial operations |

**Declaration:**

```sql
CREATE TABLE cities (id BIGINT, position GEOGRAPHY)
    TBLPROPERTIES ('format-version' = '3');
```

**Insert (WKB hex):**

```sql
-- POINT(-122.3321, 47.6062) — Seattle
INSERT INTO cities VALUES (1, X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740');
```

**Spatial Functions:**

```sql
SELECT * FROM regions
WHERE ST_INTERSECTS(boundary, point_geom);

SELECT * FROM zones
WHERE ST_CONTAINS(zone_geom, query_point);

SELECT * FROM areas
WHERE ST_WITHIN(query_point, area_geom);
```

**Notes:**
- Stored as VARBINARY internally with zero additional overhead
- Requires Iceberg format-version 3 for full support
- CRS metadata is preserved through Iceberg table properties

**See Also:** [Functions — Spatial](functions/spatial-functions.md)

---

## 8. Binary Types

### BLOB / VARBINARY

| Property | Value |
|----------|-------|
| Storage | Variable-length binary data |
| Aliases | `BLOB`, `VARBINARY`, `BINARY` |
| Literal | Hex literal `X'...'` |

```sql
CREATE TABLE binaries (id BIGINT, data BLOB);
INSERT INTO binaries VALUES (1, X'DEADBEEF');
```

---

### FIXED

| Property | Value |
|----------|-------|
| Storage | Fixed-length binary data |
| Use case | UUIDs, hashes, fixed-size encodings |

---

## 9. Other Types

### UUID

| Property | Value |
|----------|-------|
| Storage | 128-bit universally unique identifier |

---

## 10. Type Casting

### CAST Syntax

```sql
CAST(expression AS target_type)
```

**Examples:**

```sql
-- Numeric conversions
SELECT CAST(42 AS BIGINT);
SELECT CAST(3.14 AS DECIMAL(5,2));
SELECT CAST('123' AS INT);

-- String conversions
SELECT CAST(42 AS VARCHAR);
SELECT CAST(DATE '2024-01-01' AS VARCHAR);

-- Date/time conversions
SELECT CAST('2024-01-15' AS DATE);
SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP);

-- Complex type casting
SELECT CAST(embedding AS VECTOR(1024)) FROM raw_embeddings;
```

### Type Compatibility Matrix

| From \ To | INT | BIGINT | FLOAT | DOUBLE | DECIMAL | VARCHAR | BOOLEAN |
|-----------|-----|--------|-------|--------|---------|---------|---------|
| INT | - | Yes | Yes | Yes | Yes | Yes | No |
| BIGINT | Narrow | - | Yes | Yes | Yes | Yes | No |
| FLOAT | Truncate | Truncate | - | Yes | Yes | Yes | No |
| DOUBLE | Truncate | Truncate | Narrow | - | Yes | Yes | No |
| DECIMAL | Truncate | Truncate | Yes | Yes | - | Yes | No |
| VARCHAR | Parse | Parse | Parse | Parse | Parse | - | Parse |
| BOOLEAN | No | No | No | No | No | Yes | - |

**Notes:**
- "Narrow" = possible loss of precision or overflow
- "Truncate" = decimal portion is removed
- "Parse" = string must be a valid representation of the target type
- Invalid casts produce a runtime error

---

## Cross-References

| Topic | Guide |
|-------|-------|
| Vector distance functions | [Vector Search](vector-search.md) |
| AI functions using FILE type | [AI Functions](ai-functions.md) |
| SQL DDL for CREATE TABLE | [SQL Reference](sql-reference.md) |
| Complex type functions | [Functions — Array/Map/Struct](functions/array-map-struct-functions.md) |
| Spatial functions | [Functions — Spatial](functions/spatial-functions.md) |
