package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link DmlExecutor#executeMerge} throws
 * {@link IllegalStateException} when no native dispatch function is configured.
 * MERGE always requires the native execution engine; no local fallback exists.
 */
class DmlExecutorMergeFailFastTest {

    @Test
    void executeMergeThrowsWhenNativeDispatchNotConfigured() {
        // compileAndDispatch == null via defaults()
        DmlExecutor executor = new DmlExecutor(null, DmlExecutorConfig.defaults());

        MergeStatement stmt = buildMinimalMerge();

        assertThatThrownBy(() -> executor.executeMerge(stmt, null, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("MERGE requires native execution engine");
    }

    private static MergeStatement buildMinimalMerge() {
        return new MergeStatement(
                Identifier.of("cat", "schema", "target"),
                Optional.empty(),
                new TableRef(Identifier.of("cat", "schema", "source")),
                new BinaryExpression(
                        Identifier.of("id"),
                        BinaryExpression.Operator.EQUAL,
                        Literal.ofInteger(1)),
                List.of(
                        new MergeStatement.MergeClause.WhenMatchedUpdate(
                                Optional.empty(),
                                List.of(new UpdateStatement.Assignment("col",
                                        Literal.ofString("val"))))));
    }
}
