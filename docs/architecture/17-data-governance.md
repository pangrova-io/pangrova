---
title: Data Governance
sidebar_label: Data Governance
---

# Pangrova — Data Governance

> **Summary.** Data lineage, data-quality rules, and data classification.

## 1. Data Governance Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Data Governance Architecture                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Governance capabilities:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Capability         │ Responsibilities            │ Implementation      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Schema Evolution   │ Schema change management    │ Iceberg/Paimon native│   │
│  │  Data Lineage       │ Data lineage tracking       │ OpenLineage integration│   │
│  │  Data Quality       │ Data quality validation     │ Rule engine + alerts│   │
│  │  Data Classification│ Data classification         │ Tag system          │   │
│  │  Metadata Management│ Unified metadata management │ See 07-metadata-management.md │
│  │  Catalog Federation │ External Catalog federation │ See [07 §3.8](./07-metadata-management.md) CatalogAdapter SPI │ <!-- CYCLE-003: catalog federation owned by 07; 11 references 07 only -->
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Schema Evolution

### 2.1 Supported Schema Changes

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Schema Evolution Support                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Change types:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Operation          │ Iceberg │ Paimon │ Note                           │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  ADD COLUMN         │ ✓       │ ✓      │ Add a column; position can be specified│   │
│  │  DROP COLUMN        │ ✓       │ ✓      │ Drop a column; data retained   │   │
│  │  RENAME COLUMN      │ ✓       │ ✓      │ Rename a column                │   │
│  │  ALTER COLUMN TYPE  │ Partial │ Partial│ Only compatible type promotions supported│   │
│  │  REORDER COLUMNS    │ ✓       │ ✗      │ Reorder columns                │   │
│  │  ADD NESTED FIELD   │ ✓       │ ✓      │ Add field to nested struct     │   │
│  │  CHANGE NULLABILITY │ ✓       │ ✓      │ NOT NULL → NULL (not the reverse)│   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Type promotion rules (safe changes): see [07-metadata-management.md §3.7](./07-metadata-management.md)  │
│  `SchemaEvolutionValidator.SAFE_PROMOTIONS` for the authoritative rule set.      │
│                                                                                  │
<!-- Resolves: DEF-AP-001, RED-006 — Removed duplicated type promotion rules; 07 §3.7 is authoritative -->
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Schema Evolution SQL

```sql
-- Add columns
ALTER TABLE orders ADD COLUMN shipping_address STRING AFTER customer_id;
ALTER TABLE orders ADD COLUMN metadata STRUCT<created_by: STRING, updated_at: TIMESTAMP>;

-- Drop a column
ALTER TABLE orders DROP COLUMN deprecated_field;

-- Rename a column
ALTER TABLE orders RENAME COLUMN old_name TO new_name;

-- Change type (only compatible promotions supported)
ALTER TABLE orders ALTER COLUMN amount TYPE DECIMAL(20, 4);

-- Modify comment
ALTER TABLE orders ALTER COLUMN amount COMMENT 'Order amount in USD';

-- Nested struct changes
ALTER TABLE orders ALTER COLUMN metadata ADD FIELD version INT;
ALTER TABLE orders ALTER COLUMN metadata DROP FIELD deprecated;

-- View Schema history
SELECT * FROM iceberg.db.orders$history;
SELECT * FROM iceberg.db.orders$snapshots;
```

### 2.3 Schema Compatibility Check

> **Authoritative implementation**: The sole implementation of Schema change compatibility validation is `SchemaEvolutionValidator` in [07-metadata-management.md §3.7](./07-metadata-management.md). This validator acts as a pre-interceptor for `MetadataService.alterTable()`, supporting compatibility checks for five change types: AddColumn / DropColumn / RenameColumn / ChangeType / ChangeNullability.
>
> Validation rule summary:
> - **Safe changes (allowed)**: int → bigint, float → double, increasing decimal precision, increasing varchar length, non-nullable → nullable
> - **Unsafe changes (rejected)**: bigint → int, double → float, nullable → non-nullable, any narrowing type change
>
> This module (Data Governance) does not redefine validation logic; it invokes it through the unified `MetadataService` entry point.

---

## 3. Data Lineage

### 3.1 Lineage Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Data Lineage Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Based on the OpenLineage standard:                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Query Execution                                                        │   │
│  │       │                                                                  │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────────┐                                                    │   │
│  │  │ Lineage Extractor│  ← Extract lineage from the execution plan        │   │
│  │  │                 │                                                    │   │
│  │  │ • Source Tables │                                                    │   │
│  │  │ • Target Tables │                                                    │   │
│  │  │ • Transformations│                                                   │   │
│  │  │ • Column Lineage│                                                    │   │
│  │  └────────┬────────┘                                                    │   │
│  │           │                                                              │   │
│  │           ▼                                                              │   │
│  │  ┌─────────────────┐     ┌─────────────────┐                           │   │
│  │  │ OpenLineage     │ ──► │ Lineage Backend │                           │   │
│  │  │ Event           │     │ (Marquez/Datahub)│                          │   │
│  │  └─────────────────┘     └─────────────────┘                           │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Lineage Event Model

```java
/**
 * OpenLineage event emitter
 */
public class LineageEventEmitter {
    
    private final OpenLineageClient client;
    
    /**
     * Emit the START event when the query begins
     */
    public void emitStartEvent(QueryContext context, ExecutionPlan plan) {
        RunEvent event = RunEvent.builder()
            .eventType(RunEvent.EventType.START)
            .eventTime(ZonedDateTime.now())
            .run(Run.builder()
                .runId(context.getQueryId().toString())
                .build())
            .job(Job.builder()
                .namespace("pangrova")
                .name(context.getQueryId().toString())
                .facets(JobFacets.builder()
                    .sql(SqlJobFacet.builder()
                        .query(context.getSql())
                        .build())
                    .build())
                .build())
            .inputs(extractInputDatasets(plan))
            .outputs(extractOutputDatasets(plan))
            .build();
        
        client.emit(event);
    }
    
    /**
     * Extract input datasets
     */
    private List<InputDataset> extractInputDatasets(ExecutionPlan plan) {
        return plan.getSourceTables().stream()
            .map(table -> InputDataset.builder()
                .namespace(table.getCatalog())
                .name(table.getSchema() + "." + table.getTable())
                .facets(DatasetFacets.builder()
                    .schema(extractSchemaFacet(table))
                    .columnLineage(extractColumnLineage(plan, table))
                    .build())
                .build())
            .toList();
    }
    
    /**
     * Extract column-level lineage
     */
    private ColumnLineageDatasetFacet extractColumnLineage(ExecutionPlan plan, TableRef table) {
        Map<String, List<ColumnLineageField>> lineage = new HashMap<>();
        
        // Analyze the execution plan and trace the origin of each column
        for (OutputColumn output : plan.getOutputColumns()) {
            List<ColumnLineageField> sources = traceColumnSources(plan, output);
            lineage.put(output.getName(), sources);
        }
        
        return ColumnLineageDatasetFacet.builder()
            .fields(lineage)
            .build();
    }
}
```

### 3.3 Lineage Query API

<!-- Resolves: DEF-156 — Lineage query API return schema and registration -->

> **Registration**: `system.lineage` schema is registered in `InternalCatalog` alongside `system.runtime` (see [07 §2.2](./07-metadata-management.md)). These are Table-Valued Functions (TVFs) implemented as `ConnectorTableFunction` SPI extensions. The SQL parser (Calcite-based) supports TVF syntax natively via `TABLE(function(...))`.

**Return Schemas**:

| Function | Return Columns | Description |
|----------|---------------|-------------|
| `system.lineage.upstream(table_name)` | `source_table VARCHAR`, `depth INT`, `job_name VARCHAR`, `last_run_time TIMESTAMP` | Upstream dependency tables |
| `system.lineage.downstream(table_name)` | `target_table VARCHAR`, `depth INT`, `job_name VARCHAR`, `last_run_time TIMESTAMP` | Downstream consumer tables |
| `system.lineage.column_lineage(table_name, column_name)` | `source_table VARCHAR`, `source_column VARCHAR`, `transformation_type VARCHAR`, `expression VARCHAR` | Column-level lineage |
| `system.lineage.path(source, target)` | `hop INT`, `table_name VARCHAR`, `job_name VARCHAR`, `transformation VARCHAR` | Data flow path between two tables |

**Data Source**: Queries are translated to Marquez/DataHub REST API calls by `LineageConnector`, results are converted to Arrow `VectorSchemaRoot` and returned as a virtual table scan.

```sql
-- Query a table's upstream dependencies (which tables produced this table's data)
SELECT * FROM system.lineage.upstream('iceberg.warehouse.fact_orders');

-- Query a table's downstream impact (where this table's data flows to)
SELECT * FROM system.lineage.downstream('iceberg.warehouse.dim_customers');

-- Query column-level lineage
SELECT * FROM system.lineage.column_lineage('iceberg.warehouse.fact_orders', 'total_amount');

-- Query the data flow path
SELECT * FROM system.lineage.path(
    source => 'iceberg.raw.orders',
    target => 'iceberg.warehouse.fact_orders'
);
```

---

## 4. Data Quality

### 4.1 Data Quality Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Data Quality Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Quality check types:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Check Type        │ Example                     │ Trigger timing       │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Completeness      │ NOT NULL, required fields   │ On write / scheduled │   │
│  │  Uniqueness        │ Primary key unique, business key unique│ On write / scheduled │   │
│  │  Validity          │ Enum values, format validation│ On write / scheduled │   │
│  │  Consistency       │ Cross-table consistency, referential integrity│ Scheduled            │   │
│  │  Timeliness        │ Data freshness              │ Scheduled            │   │
│  │  Accuracy          │ Business rule validation    │ Scheduled            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Execution flow:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Rule Definition → Scheduler → Executor → Result Store → Alert         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Quality Rule Definition

<!-- Resolves: DEF-157 — Data quality SQL parser and executor integration -->

> **Parser Integration**: `CREATE DATA QUALITY RULE`, `SHOW DATA QUALITY RULES`, and `RUN DATA QUALITY CHECK` are Pangrova-specific SQL extensions. They are registered as custom statement rules in the ANTLR4 parser (see [00 §3.1](./01-overview.md) parser rule list). The parser produces `CreateDataQualityRule`, `ShowDataQualityRules`, and `RunDataQualityCheck` AST nodes, which are handled by `GovernanceStatementHandler` in the Coordinator's statement dispatch layer.
>
> **Executor**: `check_expression` is executed as a sub-query under a dedicated `system` principal with read-only access to the target table. The execution uses the standard query pipeline ([02 §3](./03-query-execution.md)) with `ResourceQueue = SYSTEM` to avoid competing with user queries. Sub-queries (e.g., `customer_id IN (SELECT ...)`) are supported via normal Calcite planning.
>
> **Scheduler**: Quality rule scheduling uses a lightweight `CronScheduler` (single-threaded `ScheduledExecutorService`) that evaluates cron expressions from `data_quality_rules.schedule` and submits `RUN DATA QUALITY CHECK` as internal SQL statements. This is independent of `MaintenanceScheduler` ([10 §2.10](./09-streaming.md)) which handles table maintenance only.

```sql
-- Create a quality rule
CREATE DATA QUALITY RULE order_amount_positive
ON TABLE iceberg.sales.orders
CHECK (amount > 0)
SEVERITY ERROR
MESSAGE 'Order amount must be positive';

CREATE DATA QUALITY RULE customer_email_format
ON TABLE iceberg.crm.customers
CHECK (email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
SEVERITY WARNING
MESSAGE 'Invalid email format';

CREATE DATA QUALITY RULE order_customer_exists
ON TABLE iceberg.sales.orders
CHECK (customer_id IN (SELECT id FROM iceberg.crm.customers))
SEVERITY ERROR
MESSAGE 'Customer does not exist';

-- Completeness rule
CREATE DATA QUALITY RULE orders_completeness
ON TABLE iceberg.sales.orders
CHECK (
    COUNT(*) > 0 AND
    COUNT(CASE WHEN order_id IS NULL THEN 1 END) = 0 AND
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) = 0
)
SEVERITY ERROR;

-- Freshness rule
CREATE DATA QUALITY RULE orders_freshness
ON TABLE iceberg.sales.orders
CHECK (MAX(updated_at) > CURRENT_TIMESTAMP - INTERVAL '1' HOUR)
SEVERITY WARNING
MESSAGE 'Orders data is stale';

-- View rules
SHOW DATA QUALITY RULES ON iceberg.sales.orders;

-- Manually run checks
RUN DATA QUALITY CHECK ON iceberg.sales.orders;
```

### 4.3 Quality Rule Storage

<!-- Resolves: DEF-158 — data_quality_rules and data_quality_results PostgreSQL tables registered in 01 §3.4 Shared State Responsibility Matrix. These tables belong to the PostgreSQL Shared State Layer (see [02-distributed-architecture.md §3.4](./02-distributed-architecture.md)). -->

```sql
-- Quality rules table
CREATE TABLE data_quality_rules (
    rule_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    rule_name VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    check_expression TEXT NOT NULL,           -- SQL expression
    severity VARCHAR(20) NOT NULL,            -- ERROR, WARNING, INFO
    message TEXT,
    
    enabled BOOLEAN DEFAULT TRUE,
    schedule VARCHAR(100),                    -- cron expression
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_rule (catalog_name, schema_name, table_name, rule_name)
);

-- Quality check results table
CREATE TABLE data_quality_results (
    result_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    rule_id BIGINT NOT NULL,
    check_time TIMESTAMP NOT NULL,
    
    passed BOOLEAN NOT NULL,
    total_rows BIGINT,
    failed_rows BIGINT,
    sample_failures JSON,                     -- Failure samples
    
    execution_time_ms BIGINT,
    error_message TEXT,
    
    INDEX idx_rule_time (rule_id, check_time),
    INDEX idx_time (check_time)
);
```

### 4.4 Quality Alerting

```java
/**
 * Data quality alerter
 */
public class DataQualityAlerter {
    
    private final AlertChannelRegistry channels;
    
    public void alert(QualityCheckResult result) {
        if (result.isPassed()) return;
        
        DataQualityRule rule = result.getRule();
        
        Alert alert = Alert.builder()
            .severity(rule.getSeverity())
            .title("Data Quality Check Failed: " + rule.getName())
            .message(String.format(
                "Table: %s.%s.%s\nRule: %s\nFailed Rows: %d / %d\nMessage: %s",
                rule.getCatalog(), rule.getSchema(), rule.getTable(),
                rule.getName(),
                result.getFailedRows(), result.getTotalRows(),
                rule.getMessage()))
            .timestamp(Instant.now())
            .labels(Map.of(
                "table", rule.getFullTableName(),
                "rule", rule.getName(),
                "severity", rule.getSeverity().name()))
            .build();
        
        // Send to the configured alerting channels
        for (AlertChannel channel : channels.getChannels(rule.getSeverity())) {
            channel.send(alert);
        }
    }
}

// <!-- Resolves: DEF-159 — AlertChannel SPI interface definition and AlertChannelRegistry -->
/**
 * Alert channel SPI — extensible alert delivery.
 * If designed as an SPI extension point, register in 00 §5.1 SPI summary table.
 */
interface AlertChannel {
    String name();
    void send(Alert alert);
    boolean supports(Severity severity);
}

/**
 * Registry for discovering and managing alert channels.
 */
interface AlertChannelRegistry {
    List<AlertChannel> getChannels(Severity severity);
    void register(AlertChannel channel);
}
```

---

## 5. Data Classification

### 5.1 Classification Tag System

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Data Classification                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Classification dimensions:                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Dimension         │ Tag examples                │ Purpose              │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Sensitivity level │ PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED       │   │
│  │  PII type          │ PII_NAME, PII_EMAIL, PII_PHONE, PII_SSN          │   │
│  │  Data domain       │ FINANCE, HR, SALES, MARKETING                    │   │
│  │  Compliance requirement│ GDPR, HIPAA, PCI_DSS, SOX                        │   │
│  │  Data quality      │ GOLDEN, SILVER, BRONZE                           │   │
│  │  Lifecycle         │ ACTIVE, ARCHIVED, DEPRECATED                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Tag Management SQL

```sql
-- Create a tag
CREATE TAG sensitivity_level WITH VALUES ('PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED');
CREATE TAG pii_type WITH VALUES ('PII_NAME', 'PII_EMAIL', 'PII_PHONE', 'PII_SSN', 'PII_ADDRESS');
CREATE TAG data_domain WITH VALUES ('FINANCE', 'HR', 'SALES', 'MARKETING', 'OPERATIONS');

-- Tag a table
ALTER TABLE iceberg.hr.employees SET TAG sensitivity_level = 'CONFIDENTIAL';
ALTER TABLE iceberg.hr.employees SET TAG data_domain = 'HR';

-- Tag a column
ALTER TABLE iceberg.hr.employees ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL';
ALTER TABLE iceberg.hr.employees ALTER COLUMN ssn SET TAG pii_type = 'PII_SSN';
ALTER TABLE iceberg.hr.employees ALTER COLUMN salary SET TAG sensitivity_level = 'RESTRICTED';

-- Query tags
SHOW TAGS ON TABLE iceberg.hr.employees;
SHOW TAGS ON COLUMN iceberg.hr.employees.email;

-- Search by tag
SELECT * FROM system.metadata.tables WHERE tags['sensitivity_level'] = 'CONFIDENTIAL';
SELECT * FROM system.metadata.columns WHERE tags['pii_type'] IS NOT NULL;

-- Auto-classification (based on column-name patterns)
CALL system.auto_classify(
    catalog => 'iceberg',
    schema => 'hr',
    rules => ARRAY[
        ROW('email', 'PII_EMAIL'),
        ROW('phone', 'PII_PHONE'),
        ROW('ssn', 'PII_SSN'),
        ROW('salary', 'RESTRICTED')
    ]
);
```

### 5.3 Tag and Security Policy Integration

**Event-driven mechanism**: Tag changes automatically trigger security policy updates via events, rather than relying on manual invocation or scheduled scans.

```
Tag Change Event Flow:
  1. ALTER TABLE ... SET TAG pii_type = 'PII_EMAIL'
     → MetadataService.alterTable() publishes TagChangedEvent
  2. TagBasedSecurityPolicy subscribes to TagChangedEvent
     → Check whether the new tag matches automatic policy rules
     → If matched, call ColumnMaskStore.createMask() to create a masking rule
  3. ColumnMaskStore.createMask() publishes a ColumnMaskChanged event
     (see 15-security.md §5.4 SecurityPolicyChangeEvent)
  4. Query Result Cache subscribes to ColumnMaskChanged
     → Invalidate cache entries for affected tables
     (see 04-query-optimization.md §3.3 Cache Invalidation Triggers)
```

**Event definition**:

```java
/**
 * Tag change event — published by MetadataService when tags are modified.
 * Consumed by TagBasedSecurityPolicy for auto-masking.
 */
public record TagChangedEvent(
    String catalog, String schema, String table,
    String columnName,    // null for table-level tags
    String tagKey,
    String oldValue,      // null if tag is new
    String newValue       // null if tag is removed
) {}
```

```java
/**
 * Tag-based automatic security policy
 * 
 * Trigger mechanism: event-driven (subscribes to TagChangedEvent), not polling.
 * Automatically evaluates and applies/removes security policies when tags change.
 */
public class TagBasedSecurityPolicy implements EventListener<TagChangedEvent> {
    
    private final ColumnMaskStore columnMaskStore;
    private final AuditConfigStore auditConfigStore;
    
    @Override
    public void onEvent(TagChangedEvent event) {
        if (event.columnName() != null) {
            handleColumnTagChange(event);
        } else {
            handleTableTagChange(event);
        }
    }
    
    private void handleColumnTagChange(TagChangedEvent event) {
        if (!"pii_type".equals(event.tagKey())) return;
        
        String fullTableName = event.catalog() + "." + event.schema() + "." + event.table();
        
        if (event.newValue() != null) {
            // Tag added/changed → apply mask
            String maskExpr = piiTypeToMaskExpression(event.newValue());
            if (maskExpr != null) {
                columnMaskStore.createOrUpdateMask(
                    fullTableName, event.columnName(), maskExpr, "auto_pii_mask");
            }
        } else {
            // Tag removed → remove auto-generated mask
            columnMaskStore.removeMask(fullTableName, event.columnName(), "auto_pii_mask");
        }
    }
    
    private void handleTableTagChange(TagChangedEvent event) {
        if (!"sensitivity_level".equals(event.tagKey())) return;
        String fullTableName = event.catalog() + "." + event.schema() + "." + event.table();
        
        if ("CONFIDENTIAL".equals(event.newValue()) || "RESTRICTED".equals(event.newValue())) {
            auditConfigStore.enableDetailedAudit(fullTableName);
        }
    }
    
    private String piiTypeToMaskExpression(String piiType) {
        return switch (piiType) {
            case "PII_EMAIL" -> "mask_email({column})";
            case "PII_PHONE" -> "mask_phone({column})";
            case "PII_SSN" -> "CONCAT('***-**-', RIGHT({column}, 4))";
            case "PII_NAME" -> "mask_name({column})";
            default -> null;
        };
    }
}
```

---

## 6. Configuration Summary

```yaml
# governance-config.yaml
governance:
  # Schema Evolution
  schema-evolution:
    compatibility-check: true
    allow-incompatible-changes: false
    
  # Data Lineage
  lineage:
    enabled: true
    backend: marquez  # marquez, datahub, custom
    marquez:
      url: http://marquez:5000
    emit-column-lineage: true
    
  # Data Quality
  data-quality:
    enabled: true
    default-schedule: "0 0 * * *"  # Run daily
    alert-channels:
      - type: slack
        webhook: ${SLACK_WEBHOOK}
        severity: [ERROR, WARNING]
      - type: email
        recipients: [data-team@example.com]
        severity: [ERROR]
    
  # Data Classification
  classification:
    auto-classify: true
    pii-patterns:
      email: ".*email.*"
      phone: ".*phone.*|.*mobile.*"
      ssn: ".*ssn.*|.*social.*security.*"
```

---

<!-- Resolves: DEF-AP-014 — Observability integration for data governance -->

## 7. Observability Integration

This section defines the metrics and tracing integration points for the data governance layer. For the full observability design, see [14-observability.md](./14-observability.md).

### 7.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.governance.schema_evolution.total` | SchemaEvolutionValidator | Schema evolution operations |
| `pangrova.governance.schema_evolution.rejected` | SchemaEvolutionValidator | Rejected incompatible changes |
| `pangrova.governance.lineage.events_emitted` | LineageEventEmitter | OpenLineage events emitted |
| `pangrova.governance.quality.checks.total` | DataQualityExecutor | Quality checks executed |
| `pangrova.governance.quality.checks.failed` | DataQualityExecutor | Quality checks failed |

### 7.2 Tracing Spans

- `governance.schema_check` — Schema compatibility validation (child of `query.optimize`)
- `governance.lineage.emit` — Lineage event emission (child of `query.execute`)
- `governance.quality.check` — Data quality check execution (SERVER span)

### 7.3 Log Events

- Schema evolution validation results (INFO)
- Data quality check failures with rule details (WARN)
- Tag change events triggering security policy updates (INFO)

See [14 §7](./14-observability.md) for structured logging format.


<!-- Resolves: DEF-150 — Data Lineage / OpenLineage integration with 14-observability.md -->
> **Lineage-Observability Integration (§3)**: OpenLineage events emitted by `LineageEventEmitter` are also forwarded to the observability pipeline in [14-observability.md §4](./14-observability.md) as tracing spans with `span.kind = LINEAGE`. This enables unified query tracing that includes both execution spans and lineage events. The integration point is `ObservabilityBridge.forwardLineageEvent(OpenLineageEvent)`.

<!-- Resolves: DEF-151 — TagBasedSecurityPolicy.createOrUpdateMask() cross-reference -->
> **ColumnMaskStore Integration (§5.3)**: `TagBasedSecurityPolicy` calls `ColumnMaskStore.createOrUpdateMask(tableName, columnName, maskExpression, tag)` defined in [15-security.md §5.3](./15-security.md). See DEF-165 fix in 12 for the complete `ColumnMaskStore` interface.

<!-- Resolves: DEF-152 — CatalogAdapter SPI location clarification -->
> **CatalogAdapter SPI**: The authoritative CatalogAdapter SPI definition is in [07-metadata-management.md §3.8](./07-metadata-management.md). Former references to "11 §6.2" in this document and others have been corrected. §6 of this document contains configuration summaries only, not SPI definitions.

<!-- Resolves: DEF-155 — LineageEventEmitter error handling -->
> **LineageEventEmitter Error Handling (§3.2)**: Event emission failures are handled with: (1) try-catch around HTTP POST to OpenLineage backend; (2) failed events are buffered in a bounded queue (max 1000 events); (3) exponential backoff retry (initial: 1s, max: 60s); (4) if queue is full, oldest events are dropped with `pangrova.lineage.events_dropped_total` counter incremented. Lineage emission failures never block query execution.
