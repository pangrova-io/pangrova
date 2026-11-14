---
title: "Data Governance"
sidebar_label: "Data Governance"
---

# Pangrova — Data Governance

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** Data lineage, data-quality rules, and data classification.

## Stable ownership of quality definitions and results

Production rule reads and saves use a stable controlled-table UUID. Catalog, schema
and table names are diagnostic projections from the current registry. Rename keeps
the rule; a new table under a released name cannot inherit it. A non-ACTIVE table or
replaced catalog deployment cannot execute the rule. Scheduled callbacks reload the
rule by ID before evaluating it, so a captured diagnostic name is not policy authority.

`quality_rule_identity` retains the table owner after rule deletion. A numeric rule ID
cannot be reassigned to a different table UUID; new targets require new rule IDs.
Bound definitions and audit results reference that permanent owner. Results retain
the captured UUID, including an evaluation finishing after its rule was deleted.

V101 preserves historical name-only definitions and results as unbound. It neither
claims a current table nor changes historical counts/expressions. Enabled unbound
rules fail closed in the production adapter until an administrator verifies and
explicitly binds their intended owner or retires them. Assigning every old rule the
UUID currently found at its name is not a supported migration.

These ownership checks do not qualify actual data evaluation or candidate-write
validation. Production bootstrap rejects attempted checks without an authorized
data-plane query runner; definition validity cannot produce a passing result.
Supplying the Native runner and validating the actual write candidate remain open
implementation requirements.

## Quality read contract and execution identity

Enabled rules require a persisted principal and tenant. V74 preserves old rules but
disables definitions without an execution subject; an authorized administrative
operation must assign that subject before re-enabling them. The store persists
replacement identities across restarts. An identity is not an authorization grant:
the execution adapter must recheck current privileges and policies.

`QualityReadRequest` carries catalog, schema, table, stable table UUID, predicate and COUNTS/SAMPLES
separately. Governance does not build executable SQL. Coordinator's `QualityReadSql`
uses Pangrova's production grammar and constructs an SQL AST from the parsed
expression and exact identifier components. It rejects added statements, outer
query clauses and unbound parameters, preserves read subqueries, and treats keywords
inside literals as data. Sample SQL uses the engine's LIMIT syntax. A FALSE or NULL
predicate is a violation; result shape is explicit rather than inferred from
sample column names. PostgreSQL oracle tests are not Native pipeline acceptance.

Legacy cron callbacks reload their own rule by ID, skip disabled or deleted rules,
and record failed evaluations as FAILED. A leader Boolean gates admission but
does not guarantee exactly-once execution across coordinator replacement.
`GovernanceContext.initialize(false)` permits a process host to supply a separate
durable scheduler. These primitives do not establish production SQL/REST rule
management, durable execution wiring or candidate-write validation.

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
│  │  Metadata Management│ Unified metadata management │ See metadata.md │
│  │  Catalog Federation │ External Catalog federation │ See [07 §3.8](./metadata.md) FederatedCatalog SPI │
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
│  Type promotion rules (safe changes): see [metadata.md §3.7](./metadata.md)  │
│  `SchemaEvolutionValidator.SAFE_PROMOTIONS` for the authoritative rule set.      │
│                                                                                  │

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

> **Authoritative implementation**: The sole implementation of Schema change compatibility validation is `SchemaEvolutionValidator` in [metadata.md §3.7](./metadata.md). This validator acts as a pre-interceptor for `MetadataService.alterTable()`, supporting compatibility checks for five change types: AddColumn / DropColumn / RenameColumn / ChangeType / ChangeNullability.
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



> **Registration status:** `system.lineage` virtual tables and their table-function
> registration are proposed. The current metadata module exposes
> `InternalCatalogHandle`, but no lineage table-function registration is wired.

**Return Schemas**:

| Function | Return Columns | Description |
|----------|---------------|-------------|
| `system.lineage.upstream(table_name)` | `source_table VARCHAR`, `depth INT`, `job_name VARCHAR`, `last_run_time TIMESTAMP` | Upstream dependency tables |
| `system.lineage.downstream(table_name)` | `target_table VARCHAR`, `depth INT`, `job_name VARCHAR`, `last_run_time TIMESTAMP` | Downstream consumer tables |
| `system.lineage.column_lineage(table_name, column_name)` | `source_table VARCHAR`, `source_column VARCHAR`, `transformation_type VARCHAR`, `expression VARCHAR` | Column-level lineage |
| `system.lineage.path(source, target)` | `hop INT`, `table_name VARCHAR`, `job_name VARCHAR`, `transformation VARCHAR` | Data flow path between two tables |

**Proposed data source:** A future lineage connector may translate queries to
Marquez or DataHub REST APIs and return an Arrow-backed virtual table scan. No such
connector is present in the repository today.

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



> **Parser status:** `GovernanceStatementHandler` and the underlying quality-rule
> services are implemented, but `CREATE/SHOW/RUN DATA QUALITY` AST nodes are not
> present in the parser and are not wired into SQL statement dispatch. The syntax
> below is therefore target design rather than an available SQL surface.
>
> The production coordinator uses `UnconfiguredQualityRuleExecutor`. An attempted evaluation fails and records its captured table UUID. Connecting the SQL executor requires a data-plane runner that authenticates the persisted principal and tenant and checks current access to every relation in the expression. The PostgreSQL oracle exercises SQL generation in tests.
>
> `CronScheduler` invokes the governance handler from a leader-gated callback. The handler reloads the rule by ID before evaluating it. Scheduled checks use the same configured executor as the write gate.

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

`org.pangrova.persistence.governance.postgresql.JdbcQualityRuleStore` owns the
PostgreSQL `quality_rules` and `quality_check_results` tables; governance is the
single owner of the store, while metadata decorates it with the stable
table-identity projection (`org.pangrova.metadata.governance`). V53 converges the old
CDC-prefixed tables into this authority. Rule definitions may be updated;
execution results append history. During Coordinator assembly, the PostgreSQL provider checks that the canonical rule, identity and result relations resolve before exposing the store. The check reads no rows and creates no schema objects.

A producer's `resultId=0` requests a new database-issued identity, including
across processes and store restarts. Existing explicit result IDs are preserved;
a duplicate insert reports SQLSTATE `23505` and leaves the original result intact.
V67 upgrades the result identity allocator beyond the existing positive high-water
mark, retaining legacy rows, including a row with ID zero. Apply the production
Flyway migrations before using the new writer. Fresh standalone schemas use the
same identity definition below.

```sql
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id            BIGINT        NOT NULL PRIMARY KEY,
    rule_name          VARCHAR(512)  NOT NULL,
    catalog_name       VARCHAR(255)  NOT NULL,
    schema_name        VARCHAR(255)  NOT NULL,
    table_name         VARCHAR(255)  NOT NULL,
    check_expression   TEXT          NOT NULL,
    severity           VARCHAR(32)   NOT NULL,
    message            TEXT,
    enabled            BOOLEAN       NOT NULL DEFAULT TRUE,
    schedule           VARCHAR(255),
    created_at         TIMESTAMP     NOT NULL,
    updated_at         TIMESTAMP     NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_check_results (
    result_id          BIGINT        GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    rule_id            BIGINT        NOT NULL,
    rule_name          VARCHAR(512)  NOT NULL,
    table_name         VARCHAR(255)  NOT NULL,
    severity           VARCHAR(32)   NOT NULL,
    check_time         TIMESTAMP     NOT NULL,
    passed             BOOLEAN       NOT NULL,
    total_rows         BIGINT        NOT NULL DEFAULT 0,
    failed_rows        BIGINT        NOT NULL DEFAULT 0,
    execution_time_ms  BIGINT        NOT NULL DEFAULT 0,
    error_message      TEXT
);
```

PostgreSQL's [sequence allocator](https://www.postgresql.org/docs/16/functions-sequence.html)
issues distinct values across concurrent sessions. Sequence gaps are valid;
result IDs are identities, not execution counts. Data-plane quality evaluation
and its complete SQL/CDC chain remain in the acceptance inventory.

**Write-time gate.** When governance is enabled, the coordinator installs a
`RuleBasedWriteQualityGate` on the distributed write path
(`pangrova.governance.write-quality-gate.enabled`, default `true`). Before a
write records its durable commit intent and calls the connector, the gate
evaluates every enabled rule for the target table:

- An `ERROR`-severity failure fails the query with error code
  `GOVERNANCE-0002`; the catalog is never asked to commit, and no durable write
  intent is recorded.
- `WARNING` and `INFO` failures are recorded but do not block.
- A gate that cannot evaluate its rules (store unavailable, no recoverable
  target table) also blocks: the write is never admitted on missing evidence.
- Every rule result is appended to `quality_check_results`, giving a durable
  record of the decision.

The gate uses the coordinator's configured `QualityRuleExecutor`. Bootstrap
currently wires `UnconfiguredQualityRuleExecutor`, which rejects every attempted
evaluation with an explicit missing-runner error. A nonblank expression is never
evidence that table data passed a rule. The handler and write gate append the
failed result with its captured table UUID; an enabled `ERROR` rule blocks the
write before durable commit intent or connector publication. Tables with no
enabled rules continue through the gate, and `WARNING`/`INFO` keep their documented
nonblocking severity while recording failure.

The pre-commit callback currently receives only catalog, schema and table name.
The connector has yet to publish the attempt's files. A second SELECT through
ordinary query admission sees committed rows and competes with its waiting parent
write for capacity. Evaluating a candidate requires its exact attempt, pinned inputs
and readable post-image. Neither SQL-text construction nor an administrative JDBC
connection supplies them.

Scheduled checks have a separate requirement. A persisted principal and tenant must
be resolved against current access at evaluation time. A runner must bind the rule's
table UUID, authorize all subqueries and return counts and samples from the same
authorized snapshot. The production SQL resource admits authenticated user requests.
It exposes no scheduler grant for reconstructing the stored subject. Both triggers
therefore record unavailable evaluations through the same explicit rejection today.

The governance evaluator checks each returned rule ID, table UUID, table name,
rule name, severity and row counts against the captured rule. A provider cannot
convert an ERROR failure into a WARNING or supply another rule's pass. The scheduler
admits one recurring registration per rule ID. Replacing or cancelling the registration
lets an in-flight check finish and prevents it from scheduling another occurrence.
Invalid cron expressions are rejected at registration.

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

//
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

Column-level `SET TAG` / `UNSET TAG` is wired end to end. Tags are durable, bound
to the table's stable `table_id`, and query enforcement derives classification
masks directly from those canonical bindings.

```sql
-- Create a tag
CREATE TAG sensitivity_level WITH VALUES ('PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED');
CREATE TAG pii_type WITH VALUES ('PII_NAME', 'PII_EMAIL', 'PII_PHONE', 'PII_SSN', 'PII_ADDRESS');
CREATE TAG data_domain WITH VALUES ('FINANCE', 'HR', 'SALES', 'MARKETING', 'OPERATIONS');

-- Tag a column
ALTER TABLE iceberg.hr.employees ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL';
ALTER TABLE iceberg.hr.employees ALTER COLUMN ssn SET TAG pii_type = 'PII_SSN';
ALTER TABLE iceberg.hr.employees ALTER COLUMN salary SET TAG sensitivity_level = 'RESTRICTED';

-- Remove a tag
ALTER TABLE iceberg.hr.employees ALTER COLUMN email UNSET TAG pii_type;

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

Rules enforced by the DDL path:

- The table must be `LINKED` or `MANAGED`. Tagging an `OBSERVED` table fails with
  `AccessDeniedException`; run `ALTER TABLE ... ADOPT` first.
- The column must exist; unknown columns are rejected before any write.
- Table-level `SET TAG` and `CREATE TAG` are not implemented yet; only the
  column-level statements above are accepted.
- `UNLINK` and `DROP TABLE` clear that table's durable tags, its `table_id`-scoped
  bindings, and the auto-masks derived from them.

### 5.3 Tag and Security Policy Integration

The default Metadata tag API resolves the actual canonical column through the caller's
connector session for SET, UNSET and listing; DDL delegates the same admission instead
of performing a different name/type check. An exact name wins; a case-insensitive match
must be unique. PII masks require a declared VARCHAR type even for direct API callers.
Fresh physical identity is checked before and after declaration resolution, and a
missing or changing schema fingerprint rejects the operation. Persistence receives the
captured control owner and cannot retarget to a replacement UUID. These admission
checks do not close concurrent external schema evolution after admission: durable
field-bound policies and query-bound consumers remain required.

`governance.core.classification.ColumnTagBindings` persists one `COLUMN_TAG` governance binding with stable table
identity and framed `(column, tag key)` reference. `TableControlColumnMaskStore` combines
identity-bound authored masks with mandatory PII masks derived from these bindings.
`ColumnMaskingProcessor` renders their SQL through the same typed operator contracts
used by ordinary queries. The resulting expression executes in Native.

`PostgresTableControlWriteAuthority` reads the current table/catalog identity and the same
bindings on the durable publication transaction through the Workflow provider. A background raw-artifact read cannot
bypass a canonical classification merely because another coordinator has not received
an event. Production bootstrap does not create a second event-maintained mask authority.

Deferred subjects do not reconstruct authentication groups. An enabled role-scoped
row policy or mask cannot be omitted merely because its roles are absent from the
restored direct assignments: applicability is unknown, so the read is rejected
until a current identity-context resolver can establish it. This also applies to
constant expressions without context functions and to masks outside a raw read's
projection. Matching current durable roles still render the policy; proven mask
exemptions retain their existing behavior, and disabled policies do not block.
Security-transformed replay must also match the captured policy content. Ordinary
authenticated requests retain their current group-to-role resolution.

`ColumnTagService` events carry `tableId`; its cache and retirement operations use that
captured identity. It fans out notifications and reports listener failures after fanout.
Persistence precedes notification, so a notification failure does not undo the security
policy. Restarted policy readers consult the canonical store without event replay.
`TagBasedSecurityPolicy` remains an explicit component for standalone event consumers;
it is not production authorization's durable source.

Authored masks persist their stable table ID. Name reuse cannot apply an old identity's
mask to a replacement. Unbound historical authored masks are rejected until explicitly
rebound; the upgrade does not guess ownership from a matching name. The `auto_pii_`
namespace is reserved for classification and cannot be written or deleted through the
authored-mask API.

The accepted PII values, VARCHAR requirement, NULL/short-string behavior and migration
rules are specified in [column tag SQL](../sql-reference/ddl.md#alter-table).
Table-level tags, tag catalog DDL, tag search SQL and automatic classification examples
in §5.2 remain target design; they are not implemented by this column-tag path.

Physical table rename first persists its source intent and reserves both source and
target names under one table ID. Queries, CREATE and governance mutation fail closed
while the outcome is uncertain. Registry mutation, name reservations and invalidations
commit atomically. Successful rename preserves classification identity; ambiguous
connector exceptions retain RENAMING for reconciliation, rather than inferring failure
from one observation that the old path still exists. Automatic crash reconciliation and
stable identity for authored row policies/quality rules require separate completion.

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



## 7. Observability Integration

This section defines the metrics and tracing integration points for the data governance layer. For the full observability design, see [observability.md](./observability.md).

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

See [14 §7](./observability.md) for structured logging format.



> **Lineage-Observability Integration (§3)**: OpenLineage events emitted by `LineageEventEmitter` are also forwarded to the observability pipeline in [observability.md §4](./observability.md) as tracing spans with `span.kind = LINEAGE`. This enables unified query tracing that includes both execution spans and lineage events. The integration point is `ObservabilityBridge.forwardLineageEvent(OpenLineageEvent)`.


> **ColumnMaskStore Integration (§5.3)**: `TagBasedSecurityPolicy` calls `ColumnMaskStore.createOrUpdateMask(tableName, columnName, maskExpression, tag)` defined in [security.md §5.3](./security.md). See DEF-165 fix in 12 for the complete `ColumnMaskStore` interface.


> **FederatedCatalog SPI**: The authoritative FederatedCatalog SPI definition is in [metadata.md §3.8](./metadata.md). Former references to "11 §6.2" in this document and others have been corrected. §6 of this document contains configuration summaries only, not SPI definitions.


`LineageEventEmitter` closes its backend after the final physical send and reports a
transport close failure to its owner. HTTP mode persists each accepted fact in the
outbox before returning and acknowledges only confirmed sends. LOG mode keeps a
bounded in-memory queue. When full, it retains queued events and counts the rejected
incoming event. These delivery guarantees apply to their respective provider modes.
