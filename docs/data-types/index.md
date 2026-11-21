---
title: "Data Types"
description: "Supported SQL types, groups, casting, nullability, and connector mapping caveats"
---

# Data Types

Pangrova supports scalar, nested, temporal, spatial, and AI-native SQL types. The table
below is the catalog entry point; each page documents storage, literal and cast rules,
null semantics, connector mappings, and limits.

## Type groups

| Group | Types | Notes |
|---|---|---|
| Numeric | [BOOLEAN](numeric/boolean.md), [TINYINT](numeric/tinyint.md), [SMALLINT](numeric/smallint.md), [INT / INTEGER](numeric/integer.md), [BIGINT](numeric/bigint.md), [FLOAT](numeric/float.md), [DOUBLE](numeric/double.md), [DECIMAL](numeric/decimal.md) | Integers have fixed widths; DECIMAL is exact up to precision 38 |
| String | [VARCHAR](string/varchar.md) | UTF-8; `STRING` alias; `VARCHAR(n)` declares a maximum length |
| Date and time | [DATE](date-time/date.md), [TIMESTAMP](date-time/timestamp.md), [TIMESTAMP_NS](date-time/timestamp-ns.md), [literal-only types](date-time/literal-only-types.md) | TIME, TIMESTAMP WITH TIME ZONE, and INTERVAL are covered by the literal-only page |
| Complex | [ARRAY](complex/array.md), [MAP](complex/map.md), [STRUCT](complex/struct.md) | Nested types are supported by the connectors declared on each page |
| Binary | [Binary, FIXED, UUID](binary.md) | UUID remains distinct from VARBINARY and FIXED(16) |
| Semi-structured | [VARIANT](semi-structured/variant.md) | Schema-flexible values with documented constraints |
| AI-native | [VECTOR](ai-native/vector.md), [FILE](ai-native/file.md) | Retrieval types; execution depends on configured providers and indexes |
| Spatial | [GEOMETRY](spatial/geometry.md), [GEOGRAPHY](spatial/geography.md) | WKB storage; current evaluation is planar |

## Casting and nullability

- [Type casting](type-casting.md) defines the compatibility matrix, narrowing behavior, and
  the rule that invalid casts fail at runtime.
- Column declarations are nullable by default; `NOT NULL` declares a non-nullable
  column. SQL `NULL` is distinct from zero, empty string, empty collection, and false.
  Check connector and execution-path enforcement for the schema being used.
- Use `DECIMAL` for exact monetary arithmetic; `FLOAT` and `DOUBLE` are binary
  approximations.

## Connector mapping caveats

- Iceberg stores `TINYINT` and `SMALLINT` as 32-bit `INTEGER`; reads return `INTEGER`.
- Paimon preserves integer widths but stores `GEOMETRY` and `GEOGRAPHY` as `VARBINARY`.
- `VARCHAR(n)` is a logical declaration; Iceberg and Paimon do not enforce a smaller
  length at write time.
- SQL `DECIMAL` / `NUMERIC` defaults to `(38,0)`; specifying only precision gives
  scale `0`. Connector enum-only mappings that receive no parameters use their own
  `(38,18)` default, but SQL DDL supplies parameterized types. Declare both
  precision and scale explicitly for production schemas.

Managed Lance persists structural type signatures and validates the canonical
physical schema through its Native dataset control path. A metadata mapping is
not a qualification of every nested value, format version, or read/write path.

Connector-specific pages:
[Iceberg](../getting-started/connectors/iceberg.md),
[Paimon](../getting-started/connectors/paimon.md),
[Lance](../getting-started/connectors/lance.md).
