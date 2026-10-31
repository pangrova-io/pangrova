package org.pangrova.coordinator.property.query.statement;

import net.jqwik.api.*;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for StatementDispatcher routing correctness.
 * Uses a registry-driven dispatcher where isRoutable() checks factory registration.
 *
 * <p><b>Validates: Requirements 13.1, 13.2, 13.3, 13.4</b></p>
 */
class StatementDispatcherRoutingPropertyTest {

    private final StatementDispatcher dispatcher = buildAllTypesDispatcher();

    @Property(tries = 10)
    void ddlStatementsAreAlwaysRoutable(@ForAll("ddlStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("DDL statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Property(tries = 10)
    void dmlStatementsAreAlwaysRoutable(@ForAll("dmlStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("DML statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Property(tries = 10)
    void showDescribeStatementsAreAlwaysRoutable(@ForAll("showStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("SHOW/DESCRIBE statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Property(tries = 10)
    void maintenanceStatementsAreAlwaysRoutable(@ForAll("maintenanceStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("Maintenance statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Property(tries = 10)
    void allRoutableStatementsAreRoutable(@ForAll("allRoutableStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("Statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    @Property(tries = 10)
    void dynamicTableStatementsAreAlwaysRoutable(@ForAll("dynamicTableStatements") Statement stmt) {
        assertThat(dispatcher.isRoutable(stmt))
                .as("DT statement %s must be routable", stmt.getClass().getSimpleName())
                .isTrue();
    }

    // ── Generators ──

    @Provide
    Arbitrary<Statement> ddlStatements() {
        return Arbitraries.of(
                new CreateTableStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        List.of(new CreateTableStatement.ColumnDefinition("id",
                                org.pangrova.parser.ast.type.DataTypeSpec.fromRawTypeString("INTEGER"))),
                        false, List.of(), List.of(), Optional.empty(),
                        java.util.Map.of()),
                new DropTableStatement(Identifier.of("cat", "sch", "tbl"), false),
                new AlterTableStatement.DropColumn(Identifier.of("cat", "sch", "tbl"), "col1"),
                new CreateSchemaStatement(Identifier.of("cat", "sch"), false),
                new CreateCatalogStatement("test_catalog", Optional.of("iceberg"),
                        new java.util.LinkedHashMap<>()),
                new DropCatalogStatement("test_catalog"));
    }

    @Provide
    Arbitrary<Statement> dmlStatements() {
        return Arbitraries.of(
                new InsertStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new InsertOverwriteStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        Optional.of(List.of("id")),
                        List.of(List.of(Literal.ofInteger(1)))),
                new DeleteStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        Optional.empty()),
                new UpdateStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        List.of(new UpdateStatement.Assignment("id", Literal.ofInteger(2))),
                        Optional.empty()),
                new MergeStatement(
                        Identifier.of("cat", "sch", "tbl"),
                        Optional.empty(),
                        new org.pangrova.parser.ast.TableRef(Identifier.of("cat", "sch", "src")),
                        new BinaryExpression(Identifier.of("id"),
                                BinaryExpression.Operator.EQUAL, Literal.ofInteger(1)),
                        List.of()));
    }

    @Provide
    Arbitrary<Statement> showStatements() {
        return Arbitraries.of(
                new ShowSchemasStatement("default"),
                new ShowTablesStatement(List.of("default", "public")),
                new DescribeTableStatement(Identifier.of("cat", "sch", "tbl")),
                new ShowCatalogsStatement());
    }

    @Provide
    Arbitrary<Statement> maintenanceStatements() {
        return Arbitraries.of(
                new CompactStatement(Identifier.of("cat", "sch", "tbl")),
                new VacuumStatement(Identifier.of("cat", "sch", "tbl")),
                new OptimizeStatement(Identifier.of("cat", "sch", "tbl")));
    }

    @Provide
    Arbitrary<Statement> allRoutableStatements() {
        return Arbitraries.oneOf(
                ddlStatements(), dmlStatements(), showStatements(),
                maintenanceStatements(), dynamicTableStatements());
    }

    @Provide
    Arbitrary<Statement> dynamicTableStatements() {
        return Arbitraries.of(
                new CreateDynamicTableStatement(
                        Identifier.of("dt_test"),
                        new Query(false, List.of(new AllColumns()),
                                List.of(new TableRef(Identifier.of("src"))),
                                Optional.empty(), List.of(), Optional.empty(),
                                List.of(), Optional.empty()),
                        java.time.Duration.ofMinutes(5), Optional.empty(), Optional.empty(), Map.of()),
                new AlterDynamicTableStatement(
                        Identifier.of("dt_test"),
                        new AlterDynamicTableAction.Suspend()),
                new DropDynamicTableStatement(Identifier.of("dt_test"), false),
                new ShowDynamicTablesStatement(),
                new DescribeDynamicTableStatement(Identifier.of("dt_test")));
    }

    // ── Dispatcher construction ──

    private static StatementDispatcher buildAllTypesDispatcher() {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(CreateTableStatement.class, noOp);
        tasks.put(DropTableStatement.class, noOp);
        tasks.put(AlterTableStatement.DropColumn.class, noOp);
        tasks.put(CreateSchemaStatement.class, noOp);
        tasks.put(CreateCatalogStatement.class, noOp);
        tasks.put(DropCatalogStatement.class, noOp);
        tasks.put(InsertStatement.class, noOp);
        tasks.put(InsertOverwriteStatement.class, noOp);
        tasks.put(ShowSchemasStatement.class, noOp);
        tasks.put(ShowTablesStatement.class, noOp);
        tasks.put(DescribeTableStatement.class, noOp);
        tasks.put(ShowCatalogsStatement.class, noOp);
        tasks.put(CompactStatement.class, noOp);
        tasks.put(VacuumStatement.class, noOp);
        tasks.put(OptimizeStatement.class, noOp);
        tasks.put(CreateDynamicTableStatement.class, noOp);
        tasks.put(AlterDynamicTableStatement.class, noOp);
        tasks.put(DropDynamicTableStatement.class, noOp);
        tasks.put(ShowDynamicTablesStatement.class, noOp);
        tasks.put(DescribeDynamicTableStatement.class, noOp);
        var metadataFactory = new MetadataExecutionFactory(tasks);

        StatementExecutionFactory<Statement> planFactory = new StatementExecutionFactory<>() {
            @Override
            public StatementResult execute(Statement statement, StatementExecutionContext context) {
                return new StatementResult(List.of(), List.of(), null, StatementCategory.DML);
            }

            @Override
            public Set<Class<? extends Statement>> supportedTypes() {
                return Set.of(DeleteStatement.class, UpdateStatement.class, MergeStatement.class);
            }
        };

        var context = new StatementDispatcherContext(
                null, null, List.of(metadataFactory, planFactory), List.of(), null);
        return new StatementDispatcher(context);
    }
}
