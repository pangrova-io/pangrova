package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates R7.14: DDL detection for clear error when metadata is disabled.
 * Tests that StatementDispatcher.isRoutable correctly identifies DDL statements
 * so QueryDispatcher can reject them with "DDL requires metadata.enabled=true".
 */
class StatementDispatcherDdlDisabledTest {

    private final StatementDispatcher dispatcher = buildDdlDispatcher();

    @Test
    void createTableIsRoutable() {
        Statement stmt = new CreateTableStatement(
                new Identifier(List.of("db", "orders")),
                List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("BIGINT"))),
                false, List.of(), List.of(), Optional.empty(), Map.of());
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    @Test
    void dropTableIsRoutable() {
        Statement stmt = new DropTableStatement(
                new Identifier(List.of("db", "orders")), false);
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    @Test
    void createSchemaIsRoutable() {
        Statement stmt = new CreateSchemaStatement(
                new Identifier(List.of("test_schema")), false);
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    @Test
    void isRoutableDetectsCreateTable() {
        Statement stmt = new CreateTableStatement(
                new Identifier(List.of("db", "t")),
                List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("BIGINT"))),
                false, List.of(), List.of(), Optional.empty(), Map.of());
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    @Test
    void isRoutableDetectsDropTable() {
        Statement stmt = new DropTableStatement(
                new Identifier(List.of("db", "t")), false);
        assertThat(dispatcher.isRoutable(stmt)).isTrue();
    }

    private static StatementDispatcher buildDdlDispatcher() {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(CreateTableStatement.class, noOp);
        tasks.put(DropTableStatement.class, noOp);
        tasks.put(CreateSchemaStatement.class, noOp);
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(), null);
        return new StatementDispatcher(context);
    }
}
