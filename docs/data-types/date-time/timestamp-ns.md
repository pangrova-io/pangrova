---
title: "TIMESTAMP_NS"
description: "Timestamp with nanosecond precision"
---

# TIMESTAMP_NS


| Property | Value |
|----------|-------|
| Storage | Timestamp with nanosecond precision (without timezone) |
| Use case | High-precision event logging, IoT sensor data |

```sql
CREATE TABLE high_freq_events (id BIGINT, event_time TIMESTAMP_NS);
```

`TIMESTAMP_NS` columns and casts use precision 9 during planning. Fractional
timestamp literals retain up to nine digits, including values before 1970;
`INSERT VALUES`, projections and comparisons preserve this precision through the
Native Worker. Ordinary `TIMESTAMP` and timezone timestamp columns default to
microsecond precision.

CTAS derives `TIMESTAMP_NS` for timestamp outputs with more than six fractional
digits, rather than creating a microsecond destination. New Iceberg tables whose
schema contains nanosecond timestamps use format version 3 when no version is
specified; an explicit version below 3 is rejected before table creation. This
does not upgrade existing tables. Iceberg residual predicates encode the actual
microsecond/nanosecond precision, and partition projections use signed epoch
units with checked overflow.

Timezone nanosecond table types are **not yet supported end to end**. Iceberg
metadata rejects `timestamptz_ns`, including nested fields, instead of presenting
it as a timezone-free `TIMESTAMP_NS`. CTAS rejects timezone timestamp outputs
until the destination schema can retain both timezone semantics and precision.
The ability to encode a timezone precision timestamp in Substrait alone does not
establish this storage/DDL capability.

## Unified timestamp contract: remaining implementation

Use one SPI `TimestampType(precision, timeZone)` representation, replacing the
existing boolean-only record in place. Validate precision 0–9, make timezone
semantics explicit (wall-clock timestamp versus UTC-adjusted instant), and retain
these fields through nested types. Do not add a second timestamp class or a
boolean-only compatibility constructor. Existing SQL aliases remain syntax:
`TIMESTAMP` defaults to precision 6, `TIMESTAMP_NS` resolves to precision 9 without
timezone, and timezone syntax carries the requested precision. Iceberg storage
uses micros for precision ≤6 and nanos for precision >6; metadata must still
retain the declared logical precision. A connector unable to represent either
property must reject the type, not silently narrow it.

The source inventory below identifies the contracts that must change together:

| Boundary | Sources and required change |
| --- | --- |
| SPI type and aliases | `pangrova-spi/.../data/type/primitive/temporal/TimestampType.java`, `PangrovaTypeMapping.java`, `PangrovaType.java`: precision-aware construction/signature; enum aliases cannot be the authoritative temporal metadata. |
| Type signature codec | SPI `TypeSignature.java` and planner `type/StandardTypeRegistry.java`: serialize precision with the timezone base and resolve it exactly; reject malformed/out-of-range signatures instead of ignoring parameters. |
| Direct SPI callers | `PangrovaTypeMapping`, `StandardTypeRegistry`, coordinator `query/statement/DdlTypeMapper`, and the example row in `PangrovaTypeMapping`; update all `WITHOUT_TIMEZONE`/`WITH_TIMEZONE` uses deliberately. |
| SQL DDL/AST | Parser `PangrovaSql.g4`, `ast/type/BaseType.java`, `DataTypeSpec.java`, `DataTypeSpecParser.java`, `AstDdlDmlBuilder.java`; coordinator `DdlTypeMapper`, `DdlExecutor`; carry `TIMESTAMP(p)` and timezone into column metadata. |
| Literals and casts | Parser `LiteralValue.TimestampWithTimeZoneValue`, planner `parse/ExpressionToSqlNodeConverter`, `substrait/LiteralConverter`: implement timezone literals with explicit instant/session semantics; retain precision rather than formatting through milliseconds. |
| Planner/CTAS | `PlannerTypeSystem`, planner schema compilation, `PlannedQuery.queryOutputColumnTypeNames`, coordinator `CtasTask`: derive destination types from precision-aware metadata; replace lossy string/enum paths, then remove timezone rejection only after full-chain tests pass. |
| Planner protocol mapping | `substrait/TypeMapper.mapPrimitiveType`, `toPangrovaParameterizedType`, `temporalPrecision`: retain precision/timezone in both directions; support the intended timezone SQL type and reject invalid precision rather than applying `Math.min`. |
| Iceberg | `type/IcebergTypeMapper`, `schema/IcebergSchemaBuilder`, table/column handles and output handle codec: preserve both timestamp variants and flags recursively. Keep format-version policy connector-owned. `read/IcebergTemporalSubstrait` and `write/PartitionStructProjection` already distinguish units but do not replace the missing SPI contract. |
| Paimon | `type/PaimonTypeMapper`, `PaimonToPangrovaTypeVisitors`, `handle/PaimonTableHandleFactory`, write row conversion: preserve declared precision for `TimestampType` and `LocalZonedTimestampType`; stop routing through enum-only/default-precision constructors. |
| Cross-runtime codec | Protocol `v2/common.proto::LogicalType`, Substrait precision timestamp/TZ, task result-column logical types: canonical SQL and protocol type must agree in precision/timezone. Use the existing logical-type carrier; do not invent a parallel timestamp protocol. |
| Native/read/write results | `SubstraitTypeConversion`, logical-type/result metadata, Native storage timestamp unit conversions and Java page/result decoding: retain logical timezone/precision even though the execution timestamp holds seconds+nanos; check signed int64 storage range. |
| Existing type regressions | SPI `TypeRoundTripPropertyTest`, planner `TypeSystemPropertyTest` and temporal mapping tests, Iceberg/Paimon type mapper tests: cover the single revised constructor/signature and nested round trips. |

Implement the SPI/signature/DDL change as one source migration, then wire both
connector mappings and the planner schema compiler before enabling timezone
nanosecond schemas. Required acceptance tests include p0/3/6/9 and timezone
variants, negative epoch/subsecond values, int64 nanosecond endpoints, nested
metadata round trips, CTAS schema and value preservation, predicates, partitions,
Native results and true overflow rejection. Full Java → protocol → Native →
connector → result validation is required; unit conversion tests alone are not
evidence of the complete capability.
