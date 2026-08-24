Pangrova > Guide > Functions

# Functions Reference

Pangrova provides a comprehensive set of built-in functions organized by category.

---

## Function Categories

| Category | Description | Guide |
|----------|-------------|-------|
| [Aggregate Functions](aggregate-functions.md) | COUNT, SUM, AVG, MIN, MAX, STDDEV, APPROX_COUNT_DISTINCT | Statistical aggregations |
| [Window Functions](window-functions.md) | ROW_NUMBER, RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE | Partitioned analytics |
| [String Functions](string-functions.md) | CONCAT, UPPER, LOWER, SUBSTRING, TRIM, LENGTH | Text manipulation |
| [Math Functions](math-functions.md) | ABS, MOD, POWER, SQRT, arithmetic operators | Numeric computation |
| [DateTime Functions](datetime-functions.md) | EXTRACT, INTERVAL arithmetic, DATE/TIMESTAMP literals | Temporal operations |
| [Array/Map/Struct Functions](array-map-struct-functions.md) | ARRAY[], MAP[], STRUCT(), EXPLODE, GENERATE_SERIES | Complex type operations |
| [Conditional Functions](conditional-functions.md) | CASE, COALESCE, IS NULL, BETWEEN, IN, LIKE | Conditional logic |
| [Vector Functions](vector-functions.md) | L2_DISTANCE, COSINE_DISTANCE, INNER_PRODUCT | Similarity search |
| [Spatial Functions](spatial-functions.md) | ST_INTERSECTS, ST_WITHIN, ST_CONTAINS | Geospatial operations |

---

## AI Functions

AI/ML functions are documented separately due to their breadth and distinct usage patterns:

| Guide | Content |
|-------|---------|
| [AI Functions](../ai-functions.md) | AI_COMPLETE, AI_CLASSIFY, AI_EMBED, multimodal functions, embedding model DDL |
| [Vector Search](../vector-search.md) | Distance functions in search context, RERANK BY, hybrid search |

---

## Function Naming Conventions

- Functions are **case-insensitive**: `count(*)` = `COUNT(*)` = `Count(*)`
- Qualified names supported: `catalog.schema.function_name(...)`
- AI functions use the `AI_` prefix
- Spatial functions use the `ST_` prefix
- Vector distance functions use descriptive names (no prefix)
