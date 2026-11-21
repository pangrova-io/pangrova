---
title: "Functions"
description: "Overview of Pangrova built-in function categories"
---

# Functions

Function registration in Calcite establishes name and type validation, not necessarily a
native or connector execution path. The pages below document the intended SQL contract;
runtime-dependent functions call out their provider or index requirements. Treat an
unqualified function as end-to-end supported only when its mapping and execution tests exist.

Pangrova provides a comprehensive set of built-in functions organized by category.

## Function Categories

| Category | Description | Examples |
|----------|-------------|----------|
| [Math](math.md) | Numeric computation | `ABS`, `MOD`, `POWER`, `SQRT`, arithmetic operators |
| [String](string.md) | Text manipulation | `CONCAT`, `UPPER`, `LOWER`, `SUBSTRING`, `TRIM`, `LENGTH` |
| [Date & Time](date-time.md) | Temporal operations | `EXTRACT`, `INTERVAL` arithmetic, DATE/TIMESTAMP literals |
| [Aggregate](aggregate.md) | Statistical aggregations | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `APPROX_COUNT_DISTINCT` |
| [Window](window.md) | Partitioned analytics | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` |
| [Conditional](conditional.md) | Conditional logic | `CASE`, `COALESCE`, `IS NULL`, `BETWEEN`, `IN`, `LIKE` |
| [Array](array.md) | Array construction and predicates | `ARRAY[...]`, `ARRAY_CONTAINS`, `ARRAY_CONTAINS_ALL` |
| [Map](map.md) | Map construction and access | `MAP[...]`, `map_column['key']` |
| [Struct](struct.md) | Composite value construction and access | `STRUCT(...)`, `ROW(...)`, dot notation |
| [Table Functions](table-functions.md) | Row-producing functions in `FROM` | `UNNEST`, `EXPLODE`, `POSEXPLODE`, `GENERATE_SERIES` |
| [Spatial](spatial.md) | Geospatial operations | `ST_INTERSECTS`, `ST_WITHIN`, `ST_CONTAINS` |
| [Vector](vector.md) | Embedding similarity | `L2_DISTANCE`, `COSINE_DISTANCE`, `INNER_PRODUCT` |
| [Vector Search](vector-search.md) | ANN search end-to-end | distance functions in `ORDER BY ... LIMIT`, `RERANK BY`, hybrid search |
| [AI Functions](ai-functions.md) | AI/ML integration | `AI_COMPLETE`, `AI_CLASSIFY`, `AI_EMBED`, multimodal functions, model DDL |

## Function Naming Conventions

- Functions are **case-insensitive**: `count(*)` = `COUNT(*)` = `Count(*)`
- Qualified names supported: `catalog.schema.function_name(...)`
- AI functions use the `AI_` prefix
- Spatial functions use the `ST_` prefix
- Vector distance functions use descriptive names (no prefix)
