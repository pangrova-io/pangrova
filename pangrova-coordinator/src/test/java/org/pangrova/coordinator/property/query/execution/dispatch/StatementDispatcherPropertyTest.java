package org.pangrova.coordinator.property.query.execution.dispatch;

import net.jqwik.api.*;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.*;
import org.pangrova.parser.ast.statement.dml.*;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.parser.ast.statement.stream.*;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for the unified statement dispatch path.
 *
 * <p>Verifies that {@link StatementCategory#categorize(Statement)} correctly
 * delegates to {@code StatementClassifier} and returns the expected category
 * for every known statement type.</p>
 *
 * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
 */
class StatementDispatcherPropertyTest {

    // Dispatcher with all routable types registered, for registry-driven isRoutable() tests
    private final StatementDispatcher allTypesDispatcher = buildAllTypesDispatcher();

    // ── Shared identifier generator ──

    @Provide
    Arbitrary<Identifier> qualifiedNames() {
        Arbitrary<String> names = Arbitraries.strings()
                .alpha().ofMinLength(1).ofMaxLength(12);
        return Combinators.combine(names, names, names)
                .as((cat, sch, tbl) -> Identifier.of(cat, sch, tbl));
    }


    // ── Category-specific generators ──

    @Provide
    Arbitrary<Statement> ddlStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateTableStatement(
                        name,
                        List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("INTEGER"))),
                        false, List.of(), List.of(), Optional.empty(), Map.of()),
                new DropTableStatement(name, false),
                new AlterTableStatement.AddColumn(name, "col", DataTypeSpec.fromRawTypeString("VARCHAR")),
                new AlterTableStatement.DropColumn(name, "col"),
                new AlterTableStatement.RenameColumn(name, "old", "new_col"),
                new AlterTableStatement.ChangeColumnType(name, "col", DataTypeSpec.fromRawTypeString("BIGINT")),
                new CreateSchemaStatement(Identifier.of("cat", "sch"), false),
                new CreateCatalogStatement("catalog", Optional.of("iceberg"), Map.of()),
                new DropCatalogStatement("catalog")
        ));
    }

    @Provide
    Arbitrary<Statement> dmlStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new InsertStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertOverwriteStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertSelectStatement(name, Optional.empty(), false,
                        new SetQuery(List.of(minimalQuery()))),
                new DeleteStatement(name, Optional.empty()),
                new UpdateStatement(name,
                        List.of(new UpdateStatement.Assignment("col", Literal.ofInteger(42))),
                        Optional.empty()),
                new MergeStatement(name, Optional.empty(),
                        new TableRef(Identifier.of("src")),
                        new BinaryExpression(Identifier.of("id"),
                                BinaryExpression.Operator.EQUAL, Literal.ofInteger(1)),
                        List.of())
        ));
    }

    @Provide
    Arbitrary<Statement> showStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new ShowSchemasStatement("default"),
                new ShowTablesStatement(List.of("cat", "sch")),
                new DescribeTableStatement(name),
                new ShowCatalogsStatement()
        ));
    }

    @Provide
    Arbitrary<Statement> maintenanceStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CompactStatement(name),
                new OptimizeStatement(name),
                new VacuumStatement(name)
        ));
    }

    @Provide
    Arbitrary<Statement> dynamicTableStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateDynamicTableStatement(name, minimalQuery(),
                        Duration.ofMinutes(5), Optional.empty(), Optional.empty(), Map.of()),
                new CreateContinuousQueryStatement(name, minimalQuery(),
                        Duration.ofSeconds(30)),
                new AlterDynamicTableStatement(name,
                        new AlterDynamicTableAction.Suspend()),
                new DropDynamicTableStatement(name, false),
                new ShowDynamicTablesStatement(),
                new DescribeDynamicTableStatement(name)
        ));
    }

    @Provide
    Arbitrary<Statement> unsupportedStatements() {
        return Arbitraries.of(
                new DescribeCatalogStatement("cat"),
                new AlterCatalogStatement("cat", Map.of()),
                new CallStatement(Identifier.of("proc"), List.of())
        );
    }

    @Provide
    Arbitrary<Statement> indexStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateIndexStatement("idx", name, List.of("col"),
                        "VECTOR", false, Map.of()),
                new DropIndexStatement("idx", name, false),
                new ShowIndexesStatement(name),
                new AlterIndexStatement("idx", name)
        ));
    }

    @Provide
    Arbitrary<Statement> clusteringStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new SetClusteringOrderStatement(name, List.of("col"), null),
                new ReclusterTableStatement(name, Optional.empty()),
                new ShowClusteringStatusStatement(name)
        ));
    }

    @Provide
    Arbitrary<Statement> tenantStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new GrantTenantStatement(name, Identifier.of("user")),
                new RevokeTenantStatement(name, Identifier.of("user"))
        ));
    }

    @Provide
    Arbitrary<Statement> streamStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateStreamStatement(name, false,
                        Map.of("bootstrap.servers", "localhost:9092"),
                        "debezium-json", Identifier.of("target"),
                        Map.of()),
                new AlterStreamStatement(name, new AlterStreamAction.Suspend()),
                new DropStreamStatement(name, false),
                new ShowStreamsStatement(),
                new DescribeStreamStatement(name)
        ));
    }

    @Provide
    Arbitrary<Statement> explainStatements() {
        return qualifiedNames().map(name ->
                new ExplainStatement(
                        new DeleteStatement(name, Optional.empty())));
    }

    // ── Property 1: Classification delegation preserves semantics ──

    /**
     * Property 1: Classification delegation preserves semantics.
     *
     * <p>For any DDL statement, {@code StatementCategory.categorize()} returns DDL.</p>
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void ddlStatementsCategorizeToDdl(@ForAll("ddlStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("DDL %s must categorize to DDL", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.DDL);
    }

    /**
     * Property 1 (DML variant): For any DML statement, categorize returns DML.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void dmlStatementsCategorizeToDml(@ForAll("dmlStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("DML %s must categorize to DML", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.DML);
    }

    /**
     * Property 1 (SHOW variant): For any SHOW statement, categorize returns SHOW.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void showStatementsCategorizesToShow(@ForAll("showStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("SHOW %s must categorize to SHOW", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.SHOW);
    }

    /**
     * Property 1 (MAINTENANCE variant): For any maintenance statement, categorize returns MAINTENANCE.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void maintenanceStatementsCategorizesToMaintenance(
            @ForAll("maintenanceStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Maintenance %s must categorize to MAINTENANCE", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.MAINTENANCE);
    }

    /**
     * Property 1 (DYNAMIC_TABLE variant): For any dynamic table statement, categorize returns DYNAMIC_TABLE.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void dynamicTableStatementsCategorizesToDynamicTable(
            @ForAll("dynamicTableStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("DT %s must categorize to DYNAMIC_TABLE", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.DYNAMIC_TABLE);
    }

    /**
     * Property 1 (UNKNOWN variant): For any unsupported statement, categorize returns UNKNOWN.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void unsupportedStatementsCategorizesToUnknown(
            @ForAll("unsupportedStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Unsupported %s must categorize to UNKNOWN", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.UNKNOWN);
    }

    /**
     * Property 1 (INDEX variant): For any index statement, categorize returns INDEX.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void indexStatementsCategorizesToIndex(@ForAll("indexStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Index %s must categorize to INDEX", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.INDEX);
    }

    /**
     * Property 1 (CLUSTERING variant): For any clustering statement, categorize returns CLUSTERING.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void clusteringStatementsCategorizesToClustering(
            @ForAll("clusteringStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Clustering %s must categorize to CLUSTERING", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.CLUSTERING);
    }

    /**
     * Property 1 (TENANT variant): For any tenant statement, categorize returns TENANT.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void tenantStatementsCategorizesToTenant(@ForAll("tenantStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Tenant %s must categorize to TENANT", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.TENANT);
    }

    /**
     * Property 1 (STREAM variant): For any stream statement, categorize returns STREAM.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void streamStatementsCategorizesToStream(@ForAll("streamStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("Stream %s must categorize to STREAM", stmt.getClass().getSimpleName())
                .isEqualTo(StatementCategory.STREAM);
    }

    /**
     * Property 1 (EXPLAIN variant): For any EXPLAIN statement, categorize returns EXPLAIN.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void explainStatementsCategorizesToExplain(@ForAll("explainStatements") Statement stmt) {
        assertThat(StatementCategory.categorize(stmt))
                .as("EXPLAIN must categorize to EXPLAIN")
                .isEqualTo(StatementCategory.EXPLAIN);
    }

    /**
     * Property 1 (QUERY fallthrough): A plain SELECT query categorizes to QUERY.
     *
     * <p><b>Validates: Requirements 1.1, 1.4, 1.5</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 1: Classification delegation preserves semantics")
    void plainQueryCategorizesToQuery(@ForAll("qualifiedNames") Identifier name) {
        Query query = new Query(false, List.of(new AllColumns()),
                List.of(new TableRef(name)),
                Optional.empty(), List.of(), Optional.empty(),
                List.of(), Optional.empty());
        assertThat(StatementCategory.categorize(query))
                .as("Plain SELECT must categorize to QUERY")
                .isEqualTo(StatementCategory.QUERY);
    }

    // ── Property 2: Permission mapping correctness ──

    // Generators that produce ONLY statements with extractable table names
    // (excludes CreateSchemaStatement, CreateCatalogStatement, DropCatalogStatement,
    //  ShowIndexesStatement, ShowClusteringStatusStatement, ShowDynamicTablesStatement,
    //  DescribeDynamicTableStatement which have no table name in the extractor registry)

    @Provide
    Arbitrary<Statement> dmlWithTableName() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new InsertStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertOverwriteStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertSelectStatement(name, Optional.empty(), false,
                        new SetQuery(List.of(minimalQuery()))),
                new DeleteStatement(name, Optional.empty()),
                new UpdateStatement(name,
                        List.of(new UpdateStatement.Assignment("col", Literal.ofInteger(42))),
                        Optional.empty()),
                new MergeStatement(name, Optional.empty(),
                        new TableRef(Identifier.of("src")),
                        new BinaryExpression(Identifier.of("id"),
                                BinaryExpression.Operator.EQUAL, Literal.ofInteger(1)),
                        List.of())
        ));
    }

    @Provide
    Arbitrary<Statement> deleteStatements() {
        return qualifiedNames().map(name -> new DeleteStatement(name, Optional.empty()));
    }

    @Provide
    Arbitrary<Statement> updateMergeStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new UpdateStatement(name,
                        List.of(new UpdateStatement.Assignment("col", Literal.ofInteger(42))),
                        Optional.empty()),
                new MergeStatement(name, Optional.empty(),
                        new TableRef(Identifier.of("src")),
                        new BinaryExpression(Identifier.of("id"),
                                BinaryExpression.Operator.EQUAL, Literal.ofInteger(1)),
                        List.of(new MergeStatement.MergeClause.WhenMatchedUpdate(Optional.empty(),
                                List.of(new UpdateStatement.Assignment("col", Literal.ofInteger(42))))))
        ));
    }

    @Provide
    Arbitrary<Statement> insertStatements() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new InsertStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertOverwriteStatement(name, Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertSelectStatement(name, Optional.empty(), false,
                        new SetQuery(List.of(minimalQuery())))
        ));
    }

    @Provide
    Arbitrary<Statement> dropTableStatements() {
        return qualifiedNames().map(name -> new DropTableStatement(name, false));
    }

    @Provide
    Arbitrary<Statement> ddlNonDropWithTableName() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateTableStatement(
                        name,
                        List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("INTEGER"))),
                        false, List.of(), List.of(), Optional.empty(), Map.of()),
                new AlterTableStatement.AddColumn(name, "col", DataTypeSpec.fromRawTypeString("VARCHAR")),
                new AlterTableStatement.DropColumn(name, "col"),
                new AlterTableStatement.RenameColumn(name, "old", "new_col"),
                new AlterTableStatement.ChangeColumnType(name, "col", DataTypeSpec.fromRawTypeString("BIGINT"))
        ));
    }

    @Provide
    Arbitrary<Statement> indexWithTableName() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateIndexStatement("idx", name, List.of("col"),
                        "VECTOR", false, Map.of()),
                new DropIndexStatement("idx", name, false),
                new AlterIndexStatement("idx", name)
        ));
    }

    @Provide
    Arbitrary<Statement> clusteringWithTableName() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new SetClusteringOrderStatement(name, List.of("col"), null),
                new ReclusterTableStatement(name, Optional.empty())
        ));
    }

    @Provide
    Arbitrary<Statement> dynamicTableWithTableName() {
        return qualifiedNames().flatMap(name -> Arbitraries.of(
                new CreateDynamicTableStatement(name, minimalQuery(),
                        Duration.ofMinutes(5), Optional.empty(), Optional.empty(), Map.of()),
                new DropDynamicTableStatement(name, false),
                new AlterDynamicTableStatement(name,
                        new AlterDynamicTableAction.Suspend())
        ));
    }

    private static final UserIdentity TEST_USER = new UserIdentity(
            "testuser", Set.of("dev"), Map.of(), Instant.now());

    /**
     * Property 2 (DELETE): DELETE invokes checkCanDeleteFromTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void deleteInvokesCheckCanDeleteFromTable(
            @ForAll("deleteStatements") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("DELETE must invoke checkCanDeleteFromTable")
                .containsExactly("checkCanDeleteFromTable");
    }

    /**
     * UPDATE and update-only MERGE require the SQL UPDATE privilege.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void updateAndUpdateOnlyMergeInvokeOnlyUpdatePrivilege(
            @ForAll("updateMergeStatements") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("Physical overwrite must not widen SQL UPDATE permission requirements")
                .containsExactly("checkCanUpdateTable");
    }

    /**
     * Property 2 (INSERT variants): INSERT invokes checkCanInsertIntoTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void insertInvokesCheckCanInsertIntoTable(
            @ForAll("insertStatements") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("INSERT must invoke checkCanInsertIntoTable")
                .containsExactly("checkCanInsertIntoTable");
    }

    /**
     * Property 2 (DROP TABLE): DROP TABLE invokes checkCanDropTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void dropTableInvokesCheckCanDropTable(
            @ForAll("dropTableStatements") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("DROP TABLE must invoke checkCanDropTable")
                .containsExactly("checkCanDropTable");
    }

    /**
     * Property 2 (DDL non-DROP): CREATE uses CREATE privileges; schema evolution uses ALTER.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void ddlNonDropInvokesItsExactPrivilege(
            @ForAll("ddlNonDropWithTableName") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("CREATE and ALTER must invoke their exact privilege")
                .containsExactly(stmt instanceof CreateTableStatement
                        ? "checkCanCreateTable" : "checkCanAlterTable");
    }

    @Property(tries = 100)
    void catalogDdlInvokesTheExactCatalogMutationCheck(@ForAll boolean create) {
        Statement statement = create
                ? new CreateCatalogStatement("catalog", Optional.of("lance"), Map.of())
                : new DropCatalogStatement("catalog");
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(statement,
                new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);
        assertThat(recorder.calls).containsExactly(create
                ? "checkCanCreateCatalog" : "checkCanDropCatalog");
    }

    /**
     * Property 2 (INDEX): Index statements invoke checkCanAlterTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void indexInvokesCheckCanAlterTable(
            @ForAll("indexWithTableName") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("INDEX must invoke checkCanAlterTable")
                .containsExactly("checkCanAlterTable");
    }

    /**
     * Property 2 (CLUSTERING): Clustering statements invoke checkCanAlterTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void clusteringInvokesCheckCanAlterTable(
            @ForAll("clusteringWithTableName") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("CLUSTERING must invoke checkCanAlterTable")
                .containsExactly("checkCanAlterTable");
    }

    /**
     * Property 2 (DYNAMIC_TABLE): Dynamic table statements invoke checkCanAlterTable.
     *
     * <p><b>Validates: Requirements 2.1, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 8.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 2: Permission mapping correctness")
    void dynamicTableInvokesCheckCanAlterTable(
            @ForAll("dynamicTableWithTableName") Statement stmt) {
        var recorder = new RecordingAccessControl();
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), recorder);

        assertThat(recorder.calls)
                .as("DYNAMIC_TABLE must invoke checkCanAlterTable")
                .containsExactly("checkCanAlterTable");
    }

    // ── Property 3: Null access control skips permission checks ──

    // Generator combining all statement types for null-safety testing
    @Provide
    Arbitrary<Statement> allStatements() {
        return Arbitraries.oneOf(
                dmlWithTableName(),
                ddlNonDropWithTableName(),
                dropTableStatements(),
                indexWithTableName(),
                clusteringWithTableName(),
                dynamicTableWithTableName(),
                showStatements(),
                maintenanceStatements(),
                tenantStatements(),
                streamStatements(),
                explainStatements(),
                unsupportedStatements()
        );
    }

    /**
     * Property 3 (null AccessControl): When AccessControl is null,
     * PermissionChecker.check() completes without throwing for ANY statement type.
     *
     * <p><b>Validates: Requirements 2.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 3: Null access control skips permission checks")
    void nullAccessControlSkipsPermissionChecks(
            @ForAll("allStatements") Statement stmt) {
        // Must not throw — null AccessControl means skip
        PermissionChecker.check(stmt, new CoordinatorSessionContext("permission-query", Map.of(), TEST_USER, "tenant-a"), null);
    }

    /**
     * Property 3 (null UserIdentity): When UserIdentity is null,
     * PermissionChecker.check() completes without throwing for ANY statement type.
     *
     * <p><b>Validates: Requirements 2.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 3: Null access control skips permission checks")
    void nullUserIdentitySkipsPermissionChecks(
            @ForAll("allStatements") Statement stmt) {
        var recorder = new RecordingAccessControl();
        // Must not throw — null UserIdentity means skip
        PermissionChecker.check(stmt, null, recorder);
        // No permission methods should have been called
        assertThat(recorder.calls)
                .as("No permission checks when UserIdentity is null")
                .isEmpty();
    }

    /**
     * Property 3 (both null): When BOTH AccessControl and UserIdentity are null,
     * PermissionChecker.check() completes without throwing for ANY statement type.
     *
     * <p><b>Validates: Requirements 2.3</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 3: Null access control skips permission checks")
    void bothNullSkipsPermissionChecks(
            @ForAll("allStatements") Statement stmt) {
        // Must not throw — both null means skip
        PermissionChecker.check(stmt, null, null);
    }

    // ── Property 4: Routing completeness ──

    /**
     * Generator combining all routable statement types (DDL, DML, SHOW, MAINTENANCE,
     * DYNAMIC_TABLE, INDEX, CLUSTERING, TENANT, STREAM, EXPLAIN).
     */
    @Provide
    Arbitrary<Statement> routableStatements() {
        return Arbitraries.oneOf(
                ddlStatements(),
                dmlStatements(),
                showStatements(),
                maintenanceStatements(),
                dynamicTableStatements(),
                indexStatements(),
                clusteringStatements(),
                tenantStatements(),
                streamStatements(),
                explainStatements()
        );
    }

    /**
     * Property 4: Routing completeness — for any statement that has a registered
     * factory (i.e., belongs to a routable category), {@code StatementDispatcher.isRoutable()}
     * returns true.
     *
     * <p><b>Validates: Requirements 4.3, 8.1</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 4: Routing completeness")
    void routableStatementsAreRoutable(@ForAll("routableStatements") Statement stmt) {
        assertThat(allTypesDispatcher.isRoutable(stmt))
                .as("%s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    /**
     * Property 4 (negative): A plain SELECT query is NOT routable.
     *
     * <p><b>Validates: Requirements 4.3, 8.1</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 4: Routing completeness")
    void queryStatementsAreNotRoutable(@ForAll("qualifiedNames") Identifier name) {
        Query query = new Query(false, List.of(new AllColumns()),
                List.of(new TableRef(name)),
                Optional.empty(), List.of(), Optional.empty(),
                List.of(), Optional.empty());
        assertThat(allTypesDispatcher.isRoutable(query))
                .as("Plain SELECT must NOT be routable")
                .isFalse();
    }

    // ── Property 5: Unsupported statement rejection ──

    /**
     * Property 5: Unsupported statement rejection — for any unsupported statement
     * (DescribeCatalogStatement, AlterCatalogStatement, CallStatement),
     * {@code StatementDispatcher.executeDirect()} rejects execution.
     *
     * <p>Unsupported statements categorize to UNKNOWN, so {@code isRoutable()} returns
     * false and {@code executeDirect()} throws before reaching the factory lookup.</p>
     *
     * <p><b>Validates: Requirements 8.2</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 5: Unsupported statement rejection")
    void unsupportedStatementsAreRejectedByDispatcher(
            @ForAll("unsupportedStatements") Statement stmt) {
        StatementDispatcher dispatcher = buildDispatcher(List.of(), List.of());

        assertThatThrownBy(() -> dispatcher.executeDirect(
                stmt, null, "q-unsupported",
                new CoordinatorSessionContext("q-unsupported", Map.of(), TEST_USER, null)))
                .as("Unsupported %s must be rejected", stmt.getClass().getSimpleName())
                .isInstanceOfAny(
                        UnsupportedOperationException.class,
                        org.pangrova.common.exception.PangrovaException.class);
    }

    // ── Property 6: Direct pre-parsed execution equivalence ──

    /**
     * Generator for DDL statements that the stub factory can handle.
     */
    @Provide
    Arbitrary<Statement> factoryHandledStatements() {
        return ddlStatements();
    }

    /**
     * Property 6: Direct pre-parsed execution equivalence — for any routable statement
     * with a registered factory, executing via {@code executeDirect(stmt, ...)} produces
     * a result (factory executes successfully and completion events fire).
     *
     * <p>Uses a stub factory that returns a fixed result for all DDL statements.
     * QueryStateMachine is null (pure metadata operation), so {@code toDispatchResult}
     * returns null, but the factory execution and completion events still fire.</p>
     *
     * <p><b>Validates: Requirements 5.2</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 6: Direct pre-parsed execution equivalence")
    void executeDirectProducesResultForRegisteredFactory(
            @ForAll("factoryHandledStatements") Statement stmt) {
        var recordingListener = new RecordingCompletionListener();
        StatementDispatcher dispatcher = buildDispatcher(
                List.of(new StubDdlFactory()), List.of(recordingListener));

        // executeDirect with null QueryStateMachine — factory executes, events fire
        dispatcher.executeDirect(
                stmt, null, "q-direct",
                new CoordinatorSessionContext("q-direct", Map.of(), null, null));

        // Verify factory executed and completion event fired
        assertThat(recordingListener.events)
                .as("Factory must have executed and fired completion event")
                .hasSize(1);
        assertThat(recordingListener.events.get(0).queryId())
                .isEqualTo("q-direct");
    }

    // ── Property 7: Completion event firing ──

    /**
     * Property 7: Completion event firing — for any statement successfully executed
     * through {@code StatementDispatcher}, all registered listeners receive
     * {@code onCompleted} callback with the correct queryId, statement, and result.
     *
     * <p><b>Validates: Requirements 8.4</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: statement-routing-unification, Property 7: Completion event firing")
    void completionEventFiredForAllListeners(
            @ForAll("factoryHandledStatements") Statement stmt) {
        var listener1 = new RecordingCompletionListener();
        var listener2 = new RecordingCompletionListener();
        StatementDispatcher dispatcher = buildDispatcher(
                List.of(new StubDdlFactory()), List.of(listener1, listener2));

        String queryId = "q-completion-" + stmt.getClass().getSimpleName();
        dispatcher.executeDirect(
                stmt, null, queryId,
                new CoordinatorSessionContext(queryId, Map.of(), null, null));

        // Both listeners must have received exactly one event
        assertThat(listener1.events)
                .as("Listener 1 must receive onCompleted")
                .hasSize(1);
        assertThat(listener2.events)
                .as("Listener 2 must receive onCompleted")
                .hasSize(1);

        // Verify event contents
        StatementCompletionEvent event1 = listener1.events.get(0);
        assertThat(event1.queryId()).isEqualTo(queryId);
        assertThat(event1.statement()).isSameAs(stmt);
        assertThat(event1.result()).isNotNull();
        assertThat(event1.result().category()).isEqualTo(StatementCategory.DDL);
    }

    // ── Test doubles (no mock frameworks — pangrova-rules §12) ──

    /**
     * Builds a {@link StatementDispatcher} with the given factories and listeners.
     * Uses null compiler, null resultService, and null accessControl for lightweight
     * test construction.
     */
    private static StatementDispatcher buildDispatcher(
            List<StatementExecutionFactory<?>> factories,
            List<StatementCompletionListener> listeners) {
        var context = new StatementDispatcherContext(
                null,       // compiler — not needed for executeDirect
                null,       // resultService — not needed for unit tests
                factories,
                listeners,
                null);      // accessControl — null skips permission checks
        return new StatementDispatcher(context);
    }

    /**
     * Builds a StatementDispatcher with all routable statement types registered
     * in the factory registry. Used by isRoutable() tests which are now
     * registry-driven.
     */
    private static StatementDispatcher buildAllTypesDispatcher() {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new java.util.HashMap<Class<? extends Statement>, MetadataTask<?>>();
        // DDL
        tasks.put(CreateTableStatement.class, noOp);
        tasks.put(DropTableStatement.class, noOp);
        tasks.put(AlterTableStatement.AddColumn.class, noOp);
        tasks.put(AlterTableStatement.DropColumn.class, noOp);
        tasks.put(AlterTableStatement.RenameColumn.class, noOp);
        tasks.put(AlterTableStatement.ChangeColumnType.class, noOp);
        tasks.put(AlterTableStatement.SetPartitionSpec.class, noOp);
        tasks.put(CreateSchemaStatement.class, noOp);
        tasks.put(CreateCatalogStatement.class, noOp);
        tasks.put(DropCatalogStatement.class, noOp);
        // DML (metadata path)
        tasks.put(InsertStatement.class, noOp);
        tasks.put(InsertOverwriteStatement.class, noOp);
        // SHOW
        tasks.put(ShowSchemasStatement.class, noOp);
        tasks.put(ShowTablesStatement.class, noOp);
        tasks.put(DescribeTableStatement.class, noOp);
        tasks.put(ShowCatalogsStatement.class, noOp);
        // Maintenance
        tasks.put(org.pangrova.parser.ast.statement.maintenance.CompactStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.maintenance.VacuumStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.maintenance.OptimizeStatement.class, noOp);
        // Dynamic table
        tasks.put(CreateDynamicTableStatement.class, noOp);
        tasks.put(CreateContinuousQueryStatement.class, noOp);
        tasks.put(AlterDynamicTableStatement.class, noOp);
        tasks.put(DropDynamicTableStatement.class, noOp);
        tasks.put(ShowDynamicTablesStatement.class, noOp);
        tasks.put(DescribeDynamicTableStatement.class, noOp);
        // Index
        tasks.put(CreateIndexStatement.class, noOp);
        tasks.put(DropIndexStatement.class, noOp);
        tasks.put(ShowIndexesStatement.class, noOp);
        tasks.put(AlterIndexStatement.class, noOp);
        // Clustering
        tasks.put(SetClusteringOrderStatement.class, noOp);
        tasks.put(ReclusterTableStatement.class, noOp);
        tasks.put(ShowClusteringStatusStatement.class, noOp);
        // Tenant
        tasks.put(GrantTenantStatement.class, noOp);
        tasks.put(RevokeTenantStatement.class, noOp);
        // Stream
        tasks.put(org.pangrova.parser.ast.statement.stream.CreateStreamStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.stream.AlterStreamStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.stream.DropStreamStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.stream.ShowStreamsStatement.class, noOp);
        tasks.put(org.pangrova.parser.ast.statement.stream.DescribeStreamStatement.class, noOp);
        // Unsupported (registered so they appear in registry but rejected by rejectUnsupported)
        tasks.put(DescribeCatalogStatement.class, noOp);
        tasks.put(AlterCatalogStatement.class, noOp);
        tasks.put(CallStatement.class, noOp);
        var metadataFactory = new MetadataExecutionFactory(tasks);

        // Plan types
        StatementExecutionFactory<Statement> planFactory = new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(Statement statement, StatementExecutionContext context) {
                return new StatementResult(List.of(), List.of(), null, StatementCategory.DML);
            }

            @Override
            public Set<Class<? extends Statement>> supportedTypes() {
                return Set.of(DeleteStatement.class, UpdateStatement.class,
                        MergeStatement.class, InsertSelectStatement.class,
                        ExplainStatement.class);
            }
        };

        var context = new StatementDispatcherContext(
                null, null, List.of(metadataFactory, planFactory), List.of(), null);
        return new StatementDispatcher(context);
    }

    /**
     * Hand-written stub factory that handles all DDL statement types and returns
     * a fixed {@link StatementResult}. No mock frameworks per pangrova-rules §12.
     */
    static final class StubDdlFactory implements StatementExecutionFactory<Statement> {

        @Override
        public StatementResult execute(Statement statement, StatementExecutionContext context) {
            return new StatementResult(
                    List.of(),
                    List.of("result"),
                    null,
                    StatementCategory.DDL);
        }

        @Override
        public Set<Class<? extends Statement>> supportedTypes() {
            return Set.of(
                    CreateTableStatement.class,
                    DropTableStatement.class,
                    AlterTableStatement.AddColumn.class,
                    AlterTableStatement.DropColumn.class,
                    AlterTableStatement.RenameColumn.class,
                    AlterTableStatement.ChangeColumnType.class,
                    CreateSchemaStatement.class,
                    CreateCatalogStatement.class,
                    DropCatalogStatement.class);
        }
    }

    /**
     * Hand-written recording listener that captures all completion events.
     * No mock frameworks per pangrova-rules §12.
     */
    static final class RecordingCompletionListener implements StatementCompletionListener {

        final List<StatementCompletionEvent> events = new ArrayList<>();

        @Override
        public void onCompleted(StatementCompletionEvent event) {
            events.add(event);
        }
    }

    // ── Recording AccessControl (no mock frameworks — pangrova-rules §12) ──

    /**
     * Hand-written recording implementation of {@link AccessControl} that tracks
     * which table-level permission methods were called and in what order.
     * No mock frameworks used per pangrova-rules §12.
     */
    static final class RecordingAccessControl implements AccessControl {

        final List<String> calls = new ArrayList<>();

        @Override
        public void checkCanUpdateTable(UserIdentity user, String catalog, String schema, String table) {
            calls.add("checkCanUpdateTable");
        }

        // ── TableAccessControl ──

        @Override
        public void checkCanSelectFromTable(UserIdentity user, String catalog,
                                             String schema, String table) {
            calls.add("checkCanSelectFromTable");
        }

        @Override
        public void checkCanInsertIntoTable(UserIdentity user, String catalog,
                                             String schema, String table) {
            calls.add("checkCanInsertIntoTable");
        }

        @Override
        public void checkCanDeleteFromTable(UserIdentity user, String catalog,
                                             String schema, String table) {
            calls.add("checkCanDeleteFromTable");
        }

        @Override
        public void checkCanCreateTable(UserIdentity user, String catalog,
                                         String schema, String table) {
            calls.add("checkCanCreateTable");
        }

        @Override
        public void checkCanDropTable(UserIdentity user, String catalog,
                                       String schema, String table) {
            calls.add("checkCanDropTable");
        }

        @Override
        public void checkCanAlterTable(UserIdentity user, String catalog,
                                        String schema, String table) {
            calls.add("checkCanAlterTable");
        }

        @Override
        public void checkCanRenameTable(UserIdentity user, String catalog,
                                         String schema, String oldTable,
                                         String newCatalog, String newSchema,
                                         String newTable) {
            calls.add("checkCanRenameTable");
        }

        @Override
        public void checkCanShowColumns(UserIdentity user, String catalog,
                                         String schema, String table) {
            calls.add("checkCanShowColumns");
        }

        @Override
        public void checkCanSelectFromColumns(UserIdentity user, String catalog,
                                               String schema, String table,
                                               Set<String> columns) {
            calls.add("checkCanSelectFromColumns");
        }

        // ── CatalogAccessControl ──

        @Override
        public void checkCanAccessCatalog(UserIdentity user, String catalog) {
            calls.add("checkCanAccessCatalog");
        }

        @Override
        public void checkCanCreateCatalog(UserIdentity user, String catalog) {
            calls.add("checkCanCreateCatalog");
        }

        @Override
        public void checkCanDropCatalog(UserIdentity user, String catalog) {
            calls.add("checkCanDropCatalog");
        }

        // ── SchemaAccessControl ──

        @Override
        public void checkCanAccessSchema(UserIdentity user, String catalog,
                                          String schema) {
            calls.add("checkCanAccessSchema");
        }

        @Override
        public void checkCanCreateSchema(UserIdentity user, String catalog,
                                          String schema) {
            calls.add("checkCanCreateSchema");
        }

        @Override
        public void checkCanDropSchema(UserIdentity user, String catalog,
                                        String schema) {
            calls.add("checkCanDropSchema");
        }

        @Override
        public void checkCanAlterSchema(UserIdentity user, String catalog,
                                         String schema) {
            calls.add("checkCanAlterSchema");
        }

        @Override
        public void checkCanShowSchemas(UserIdentity user, String catalog) {
            calls.add("checkCanShowSchemas");
        }

        @Override
        public void checkCanShowTables(UserIdentity user, String catalog,
                                        String schema) {
            calls.add("checkCanShowTables");
        }

        // ── ColumnAccessControl ──

        @Override
        public Optional<String> getRowFilter(UserIdentity user, String catalog,
                                              String schema, String table) {
            calls.add("getRowFilter");
            return Optional.empty();
        }

        @Override
        public Optional<String> getColumnMask(UserIdentity user, String catalog,
                                               String schema, String table,
                                               String column) {
            calls.add("getColumnMask");
            return Optional.empty();
        }
    }

    // ── Helper ──

    private static Query minimalQuery() {
        return new Query(false, List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of("src"))),
                Optional.empty(), List.of(), Optional.empty(),
                List.of(), Optional.empty());
    }
}
