---
title: "Developer Guide"
sidebar_label: "Developer Guide"
---

# Pangrova — Developer Guide

> **Summary.** Development-environment setup, the module development workflow, and connector development.

> **Module inventory:** The project contains 38 Maven modules (the root aggregator
> plus 37 sub-modules, four of which are group aggregators). For the authoritative
> module list, see [Operations](../operations/index.md) and the
> `<modules>` section of the root `pom.xml`.

## 1. Development Environment Setup

### 1.1 System Requirements

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Development Environment Requirements                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Required Software:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Software           │ Version       │ Purpose                           │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  JDK                │ 17+           │ Java compile and run              │   │
│  │  Maven              │ 3.8+          │ Build management                  │   │
│  │  CMake              │ 3.20+         │ Native build                      │   │
│  │  GCC/Clang          │ 11+/14+       │ C++ compile                       │   │
│  │  Docker             │ 20+           │ Test containers                   │   │
│  │  Git                │ 2.30+         │ Version control                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Optional Software:                                                              │
│  • IntelliJ IDEA (recommended IDE)                                              │
│  • CLion (Native development)                                                   │
│  • Docker Compose (local cluster)                                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Quick Start

```bash
# 1. Clone the code
git clone https://github.com/pangrova/pangrova.git
cd pangrova

# 2. Install dependencies (macOS)
brew install cmake ninja ccache protobuf

# 3. Build Java modules
mvn clean install -DskipTests

# 4. Build the Native module
cd pangrova-worker-native
mkdir build && cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release ..
ninja

# 5. Run tests
mvn test
cd pangrova-worker-native/build && ctest

# 6. Start a local cluster
./bin/start-local-cluster.sh
```

### 1.3 IDE Configuration

```xml


<code_scheme name="Pangrova" version="173">
  <JavaCodeStyleSettings>
    <option name="CLASS_COUNT_TO_USE_IMPORT_ON_DEMAND" value="999" />
    <option name="NAMES_COUNT_TO_USE_IMPORT_ON_DEMAND" value="999" />
  </JavaCodeStyleSettings>
  <codeStyleSettings language="JAVA">
    <option name="RIGHT_MARGIN" value="120" />
    <option name="KEEP_BLANK_LINES_IN_CODE" value="1" />
    <indentOptions>
      <option name="INDENT_SIZE" value="4" />
      <option name="TAB_SIZE" value="4" />
    </indentOptions>
  </codeStyleSettings>
</code_scheme>
```

## 2. Module Development Guide

### 2.1 Module Structure Convention

```
pangrova-{module}/
├── src/
│   ├── main/
│   │   ├── java/org/pangrova/{module}/
│   │   │   ├── {Module}Module.java      # Module entry point (DI module — see [00 §9](../architecture/overview.md) for DI framework choice)

│   │   │   ├── {Module}Config.java      # Configuration class
│   │   │   ├── api/                     # Public API
│   │   │   ├── internal/                # Internal implementation
│   │   │   └── spi/                     # SPI interfaces (if any)
│   │   └── resources/
│   │       └── META-INF/services/       # SPI registration
│   └── test/
│       └── java/org/pangrova/{module}/
│           ├── Test{Feature}.java       # Unit test
│           └── Integration{Feature}Test.java  # Integration test
├── pom.xml
└── README.md
```

### 2.2 Code Conventions

```java
/**
 * Class design convention example
 * 
 * Rules:
 * 1. Single responsibility per class, no more than 500 lines
 * 2. Methods no longer than 20 lines
 * 3. No more than 5 parameters
 * 4. No more than 3 levels of indentation
 */
public class QueryExecutor {
    
    // 1. Static constants
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    
    // 2. Dependencies (final, constructor injection)
    private final QueryParser parser;
    private final QueryOptimizer optimizer;
    private final ExecutionEngine engine;
    
    // 3. Constructor (parameter validation)
    public QueryExecutor(QueryParser parser, 
                         QueryOptimizer optimizer,
                         ExecutionEngine engine) {
        this.parser = requireNonNull(parser, "parser is null");
        this.optimizer = requireNonNull(optimizer, "optimizer is null");
        this.engine = requireNonNull(engine, "engine is null");
    }
    
    // 4. Public methods (ordered by importance)
    public QueryResult execute(String sql, QueryContext context) {
        // Keep the method body concise; extract complex logic into private methods
        Statement statement = parse(sql);
        ExecutionPlan plan = optimize(statement, context);
        return executeInternal(plan, context);
    }
    
    // 5. Private methods (ordered by call order)
    private Statement parse(String sql) { /* ... */ }
    private ExecutionPlan optimize(Statement stmt, QueryContext ctx) { /* ... */ }
    private QueryResult executeInternal(ExecutionPlan plan, QueryContext ctx) { /* ... */ }
}
```

### 2.3 Comment Conventions

```java
/**
 * Class comment: describe the class's responsibility and how to use it
 * 
 * <p>Detailed description of the class's functionality, thread safety, usage constraints, etc.
 * 
 * <p>Example usage:
 * <pre>{@code
 * QueryExecutor executor = new QueryExecutor(parser, optimizer, engine);
 * QueryResult result = executor.execute("SELECT * FROM t", context);
 * }</pre>
 * 
 * @see RelatedClass
 */
public class QueryExecutor {
    
    /**
     * Method comment: describe the method's functionality, parameters, return value, exceptions
     * 
     * @param sql     SQL statement, must not be null or empty
     * @param context query context, containing session information
     * @return query result, never null
     * @throws QueryException if the SQL syntax is invalid or execution fails
     */
    public QueryResult execute(String sql, QueryContext context) {
        // Implementation comment: explain WHY, not WHAT
        // Use the cached execution plan to avoid re-optimizing the same query
        ExecutionPlan plan = planCache.get(sql, () -> optimize(sql));
        return executeInternal(plan, context);
    }
}
```

## 3. Connector Development Guide

### 3.1 Connector Development Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Connector Development Flow                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. Create the module                                                           │
│     pangrova-connector-{name}/                                                    │
│                                                                                  │
│  2. Implement core interfaces                                                   │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  ConnectorFactory → Connector → ConnectorMetadata                   │    │
│     │                              → ConnectorSplitManager                │    │
│     │                              → ConnectorPageSourceProvider          │    │
│     │                              → ConnectorPageSinkProvider (optional)  │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  3. Register SPI                                                                 │
│     META-INF/services/org.pangrova.spi.connector.ConnectorFactory                │
│                                                                                  │
│  4. Write tests                                                                 │
│     Use TestContainers for integration testing                                 │
│                                                                                  │
│  5. Documentation                                                               │
│     README.md + configuration notes                                            │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Connector Implementation Example

```java
/**
 * Example: implement a simple CSV Connector
 */

// 1. ConnectorFactory
public class CsvConnectorFactory implements ConnectorFactory {
    
    @Override
    public String getName() {
        return "csv";
    }
    
    @Override
    public Connector create(String catalogName, 
                            Map<String, String> config, 
                            ConnectorContext context) {
        CsvConfig csvConfig = new CsvConfig(config);
        return new CsvConnector(csvConfig, context);
    }
    
    @Override
    public List<PropertyMetadata<?>> getConfigProperties() {
        return List.of(
            PropertyMetadata.stringProperty("csv.path", "CSV files directory", true),
            PropertyMetadata.stringProperty("csv.delimiter", "Field delimiter", ","),
            PropertyMetadata.booleanProperty("csv.header", "Has header row", true)
        );
    }
}

// 2. Connector
public class CsvConnector implements Connector {
    
    private final CsvConfig config;
    private final CsvMetadata metadata;
    private final CsvSplitManager splitManager;
    private final CsvPageSourceProvider pageSourceProvider;
    
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }
    
    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }
    
    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }
}

// 3. ConnectorMetadata
public class CsvMetadata implements ConnectorMetadata {
    
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, 
                                            Optional<String> schemaName) {
        // Scan the directory; treat each CSV file as a table
        return Files.list(config.getPath())
            .filter(p -> p.toString().endsWith(".csv"))
            .map(p -> new SchemaTableName("default", getTableName(p)))
            .collect(Collectors.toList());
    }
    
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                   ConnectorTableHandle table) {
        CsvTableHandle csvTable = (CsvTableHandle) table;
        // Infer the schema from the CSV header
        List<ColumnMetadata> columns = inferSchema(csvTable.getPath());
        return new ConnectorTableMetadata(csvTable.getTableName(), columns);
    }
}

// 4. ConnectorSplitManager
public class CsvSplitManager implements ConnectorSplitManager {
    
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle table,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint) {
        CsvTableHandle csvTable = (CsvTableHandle) table;
        
        // Treat each CSV file as one Split
        // Large files can be split into multiple Splits by row count
        List<CsvSplit> splits = createSplits(csvTable.getPath());
        
        return new FixedSplitSource(splits);
    }
}

// 5. ConnectorPageSourceProvider
public class CsvPageSourceProvider implements ConnectorPageSourceProvider {
    
    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                                                ConnectorSession session,
                                                ConnectorSplit split,
                                                ConnectorTableHandle table,
                                                List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter) {
        CsvSplit csvSplit = (CsvSplit) split;
        CsvTableHandle csvTable = (CsvTableHandle) table;
        
        return new CsvPageSource(
            csvSplit.getPath(),
            csvSplit.getStart(),
            csvSplit.getLength(),
            columns,
            config
        );
    }
}
```

### 3.3 Connector Testing

```java
/**
 * Connector integration test example
 * Uses real objects and real dependencies; Mock/Stub/Test Double are prohibited
 */
public class CsvConnectorIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private CsvConnector connector;
    
    @BeforeEach
    void setup() throws IOException {
        // Create a test CSV file
        Path csvFile = tempDir.resolve("test.csv");
        Files.writeString(csvFile, """
            id,name,value
            1,Alice,100
            2,Bob,200
            3,Charlie,300
            """);
        
        // Create a real Connector
        connector = new CsvConnectorFactory().create(
            "csv_test",
            Map.of("csv.path", tempDir.toString()),
            new TestConnectorContext()
        );
    }
    
    @Test
    void testListTables() {
        ConnectorMetadata metadata = connector.getMetadata(testSession());
        List<SchemaTableName> tables = metadata.listTables(testSession(), Optional.empty());
        
        assertThat(tables).containsExactly(new SchemaTableName("default", "test"));
    }
    
    @Test
    void testScan() {
        // Get table metadata
        ConnectorMetadata metadata = connector.getMetadata(testSession());
        ConnectorTableHandle table = metadata.getTableHandle(
            testSession(), new SchemaTableName("default", "test"));
        
        // Get Splits
        ConnectorSplitSource splitSource = connector.getSplitManager().getSplits(
            null, testSession(), table, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        List<ConnectorSplit> splits = getAllSplits(splitSource);
        
        // Read data
        List<ColumnHandle> columns = List.copyOf(
            metadata.getColumnHandles(testSession(), table).values());
        
        List<Page> pages = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            ConnectorPageSource pageSource = connector.getPageSourceProvider()
                .createPageSource(null, testSession(), split, table, columns, DynamicFilter.EMPTY);
            
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) pages.add(page);
            }
        }
        
        // Verify the result
        long totalRows = pages.stream().mapToLong(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(3);
    }
}
```

## 4. Connector Development Quickstart

Step-by-step guide from project creation to running integration tests. Assumes JDK 17+, Maven 3.8+, Docker installed.

### Step 1: Create Module

```bash
# From pangrova root directory
mkdir -p pangrova-connector-mydb/src/main/java/org/pangrova/connector/mydb
mkdir -p pangrova-connector-mydb/src/main/resources/META-INF/services
mkdir -p pangrova-connector-mydb/src/test/java/org/pangrova/connector/mydb
```

### Step 2: Add pom.xml

```xml

<project>
    <parent>
        <groupId>org.pangrova</groupId>
        <artifactId>pangrova-connectors</artifactId>
        <version>${revision}</version>
    </parent>
    <artifactId>pangrova-connector-mydb</artifactId>

    <dependencies>
        <dependency>
            <groupId>org.pangrova</groupId>
            <artifactId>pangrova-spi</artifactId>
        </dependency>
        <dependency>
            <groupId>org.pangrova</groupId>
            <artifactId>pangrova-test-common</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

### Step 3: Implement ConnectorFactory

```java
package org.pangrova.connector.mydb;

public class MyDbConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "mydb";
    }

    @Override
    public Connector create(String catalogName,
                            Map<String, String> config,
                            ConnectorContext context) {
        MyDbConfig myDbConfig = new MyDbConfig(config);
        return new MyDbConnector(myDbConfig);
    }

    @Override
    public List<PropertyMetadata<?>> getConfigProperties() {
        return List.of(
            PropertyMetadata.stringProperty("mydb.url", "JDBC URL", true),
            PropertyMetadata.stringProperty("mydb.user", "Username", true)
        );
    }
}
```

### Step 4: Implement Core Interfaces

Implement these interfaces in order (each depends on the previous):

| Order | Interface | Responsibility |
|-------|-----------|----------------|
| 1 | `Connector` | Return metadata, split manager, page source provider |
| 2 | `ConnectorMetadata` | List schemas/tables, get column info |
| 3 | `ConnectorSplitManager` | Partition table into splits |
| 4 | `ConnectorPageSourceProvider` | Read data from splits into Arrow pages |
| 5 | `ConnectorPageSinkProvider` (optional) | Write data |

Refer to the CSV Connector example in §3.2 for implementation patterns.

### Step 5: Register SPI

```bash
# Create SPI registration file
echo "org.pangrova.connector.mydb.MyDbConnectorFactory" > \
  pangrova-connector-mydb/src/main/resources/META-INF/services/org.pangrova.spi.connector.ConnectorFactory
```

### Step 6: Write Integration Test

```java
package org.pangrova.connector.mydb;

public class MyDbConnectorIntegrationTest {

    @Container
    static final GenericContainer<?> MY_DB = new GenericContainer<>("mydb:latest")
        .withExposedPorts(3306);

    private MyDbConnector connector;

    @BeforeEach
    void setup() {
        connector = (MyDbConnector) new MyDbConnectorFactory().create(
            "test_catalog",
            Map.of(
                "mydb.url", "jdbc:mydb://localhost:" + MY_DB.getMappedPort(3306),
                "mydb.user", "test"
            ),
            new TestConnectorContext()
        );
    }

    @Test
    void listTables_returnsExpectedTables() {
        List<SchemaTableName> tables = connector.getMetadata(testSession())
            .listTables(testSession(), Optional.of("public"));
        assertThat(tables).isNotEmpty();
    }

    @Test
    void scan_returnsCorrectRowCount() {
        // Follow the pattern in §3.3 CsvConnectorIntegrationTest.testScan()
    }
}
```

### Step 7: Build and Test

```bash
# Build
mvn clean install -pl pangrova-connector-mydb -DskipTests

# Run unit tests
mvn test -pl pangrova-connector-mydb -Dtest.type=unit

# Run integration tests (requires Docker)
mvn test -pl pangrova-connector-mydb -Dtest.type=integration
```

### Step 8: Deploy

```yaml
# Add catalog config: etc/catalog/mydb.yaml
connector:
  name: mydb
  mydb:
    url: "jdbc:mydb://prod-host:3306/warehouse"
    user: "${ENV:MYDB_USER}"
```

Restart Coordinator and Workers. Verify with:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM mydb;
SELECT * FROM mydb.public.my_table LIMIT 10;
```

---

## 5. Testing Standards

For the complete standards covering testing principles, the layered strategy,
Testcontainers usage, and fuzz testing, see the [testing guide](testing.md).

- Testing constraint (mandatory): all test levels must use a real environment and real dependencies; Test Double / Stub / Mock and their variant implementations are prohibited.
- Runtime faking frameworks such as Mockito/PowerMock/EasyMock are prohibited; if external dependencies are needed, use TestContainers or a reproducible embedded real service.


### 5.1 Test Naming Convention

```java
/**
 * Test naming convention: methodName_scenario_expectedBehavior
 */
public class QueryExecutorTest {
    
    @Test
    void execute_validSelectQuery_returnsResults() { }
    
    @Test
    void execute_syntaxError_throwsQueryException() { }
    
    @Test
    void execute_tableNotFound_throwsTableNotFoundException() { }
    
    @Test
    void execute_timeout_throwsTimeoutException() { }
    
    @Test
    void execute_nullSql_throwsNullPointerException() { }
}
```

## 6. Code Submission Conventions

### 6.1 Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>

# Type:
# - feat: New feature
# - fix: Bug fix
# - docs: Documentation update
# - style: Code formatting (does not affect functionality)
# - refactor: Refactoring (neither a new feature nor a fix)
# - perf: Performance optimization
# - test: Test-related
# - chore: Build/tooling-related

# Example:
feat(parser): add support for TIME TRAVEL syntax

Implement FOR SYSTEM_TIME AS OF and FOR VERSION AS OF clauses
for Iceberg time travel queries.

Closes #123
```

### 6.2 PR Checklist

```markdown
## PR Checklist

- [ ] Code conforms to project conventions
- [ ] Added necessary unit tests
- [ ] Added necessary integration tests
- [ ] All tests pass
- [ ] Updated related documentation
- [ ] No new compiler warnings introduced
- [ ] No new dependencies introduced (or reason explained)
```

## 7. Debugging Tips

### 7.1 Local Debugging

```bash
# Start Coordinator (debug mode)
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
     -jar pangrova-coordinator/target/pangrova-coordinator.jar

# Start Worker (debug mode)
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 \
     -jar pangrova-worker/target/pangrova-worker.jar

# Native debugging (GDB)
gdb --args ./pangrova-worker-native/build/pangrova_worker_native
```

### 7.2 Log Debugging

```yaml
# Dynamically adjust log level
logging:
  level:
    org.pangrova.coordinator.query: DEBUG
    org.pangrova.worker.execution: DEBUG
    org.pangrova.connector.iceberg: TRACE
```

```sql
-- Adjust log level via SQL
SET SESSION log_level = 'DEBUG';
SET SESSION log_level FOR 'org.pangrova.planner' = 'TRACE';
```

---



## 8. Observability Integration

This section describes how developers should integrate observability into new components. For the full observability design, see [observability.md](../architecture/observability.md).

### 8.1 Adding Metrics

Use `MetricsProvider` SPI ([14 §2.1](../architecture/observability.md)) to create metrics:

```java
LongCounter counter = metricsProvider.createCounter(
    "pangrova.mycomponent.operations", "Total operations", "1");
```

Follow the `pangrova.{component}.*` naming convention defined in [14 §3](../architecture/observability.md).

### 8.2 Adding Tracing

Use `TracingProvider` SPI ([14 §2.1](../architecture/observability.md)) to create spans:

```java
Span span = tracingProvider.startSpan("mycomponent.operation", SpanKind.INTERNAL);
```

Attach spans as children of the appropriate parent in the span hierarchy ([14 §8.2](../architecture/observability.md)).

### 8.3 Structured Logging

Follow the structured logging format defined in [14 §7](../architecture/observability.md). Always include `trace_id` and `span_id` fields for log-trace correlation.



> **Module Structure (§2.1)**: Each connector module follows the convention: `src/main/java/org/pangrova/connector/{format}/spi/` for SPI interfaces and `src/main/java/org/pangrova/connector/{format}/impl/` for implementations. This aligns with [connectors.md §2](../architecture/connectors.md) ClassLoader isolation — SPI packages are loaded by the parent ClassLoader.


> **Code Standards (§2.2)**: The authoritative code standards are defined in [overview.md §8.3](../architecture/overview.md). This section provides developer-oriented quick reference only. Any divergence should be resolved in favor of 00 §8.3.


> **Connector Example (§3.2)**: `ConnectorFactory.create()` signature matches [04 §1.1](../architecture/connectors.md). `getConfigProperties()` returns `Map<String, String>` (not `Properties`) to align with the Connector SPI contract in 04 §1.2.

---

## Appendix: Quick Start (English)

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| JDK | 17+ | Java compilation and runtime |
| Maven | 3.8+ | Build management |
| Git | 2.30+ | Version control |
| Docker | 20+ | Integration tests (TestContainers) |
| CMake + Ninja | 3.20+ | Native C++ build (optional) |

### Build & Run

```bash
mvn clean package -DskipTests          # Full build
bin/pangrova-standalone.sh               # Start standalone
bin/pangrova-cli.sh                      # Interactive SQL
```

### Running Tests

```bash
mvn test -pl pangrova-coordinator -am                  # Unit tests
mvn test -pl pangrova-testing/pangrova-test-e2e -am      # E2E tests (Docker required)
mvn test -pl pangrova-testing/pangrova-test-benchmark -am # TPC-H benchmark
```

### Code Conventions

- **Style**: Checkstyle at `config/checkstyle/`
- **Bugs**: SpotBugs at `config/spotbugs/`
- **SPI**: All public interfaces under `pangrova-spi` must have `@Stable`/`@Experimental`/`@Internal`
- **License**: All new Java files require Apache 2.0 header

### Native (C++) Build

```bash
./pangrova-worker-native/bin/setup-native-build-env.sh  # One-time (builds abseil→protobuf→gRPC→Velox)
source ~/.pangrova/env.sh                                # Activate environment
cmake --preset dev-release -S pangrova-worker-native     # Configure
cmake --build pangrova-worker-native/build --parallel    # Build
ctest --test-dir pangrova-worker-native/build            # Test
./pangrova-worker-native/bin/package-native-lib.sh       # Package for Java
```

CMake presets: `dev-release` (recommended), `dev-debug` (C++ debugging only), `ci-linux-x86_64`, `ci-osx-aarch64`.

Dependencies are hermetic (`~/.pangrova/`). System paths blocked via `CMAKE_IGNORE_PATH`.
