package org.pangrova.coordinator.query.execution.dispatch;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.Fixture;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.spi.connector.base.ConnectorException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.arithmeticPredicate;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.deleteAll;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.deleteWhere;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.equalToSeven;

/** DELETE cannot invoke unfenced connector deletion; planner and write errors propagate. */
class PlanExecutionFactoryDeleteFailureTest {

    @ParameterizedTest(name = "metadata failure, partitioned={0}, all={1}, failure={2}")
    @MethodSource("metadataFailures")
    void metadataDeleteIsNeverInvokedEvenWhenConnectorWouldFail(
            boolean partitioned, boolean all, RuntimeException failure) {
        var fixture = new Fixture(partitioned);
        fixture.metadata.deleteFailure = failure;
        var statement = all ? deleteAll() : deleteWhere(equalToSeven("id"));

        PlanExecutionFactoryDeleteTestSupport.assertDmlResult(fixture.execute(statement));
        assertThat(fixture.metadata.filters).isEmpty();
        fixture.assertNativePending("dispatch");
    }

    @ParameterizedTest(name = "invalid column, partitioned={0}, predicate={1}")
    @MethodSource("invalidColumns")
    void invalidColumnFailsRealPlannerValidationWithoutConnectorDeletion(
            boolean partitioned, Expression predicate) {
        var fixture = new Fixture(partitioned);
        fixture.onDispatch = statement -> PlanExecutionFactoryDeleteCompilationTest
                .buildCompiler(fixture).compileStatementToPlannedQuery(statement);

        PangrovaException failure = assertThrows(PangrovaException.class,
                () -> fixture.execute(deleteWhere(predicate)));

        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE);
        assertThat(failure).hasMessageContaining("missing_column");
        assertThat(fixture.metadata.filters).isEmpty();
        fixture.assertNativePending("dispatch");
    }

    @ParameterizedTest(name = "null native dispatch, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void nullDispatchFailsInsteadOfReportingDeleteSuccess(boolean partitioned) {
        var fixture = new Fixture(partitioned);
        fixture.metadata.deleteFailure = new UnsupportedOperationException("Metadata DELETE is unsupported");
        fixture.dispatchResult = null;

        PangrovaException failure = assertThrows(PangrovaException.class, () -> fixture.execute(deleteAll()));

        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.RUNTIME_TASK_FAILED);
        assertThat(failure).hasMessageContaining("Native DML dispatch returned null");
        assertThat(fixture.metadata.filters).isEmpty();
        fixture.assertNativePending("dispatch");
    }

    @ParameterizedTest(name = "overwrite failure, partitioned={0}, statement={1}, failure={2}")
    @MethodSource("overwriteFailures")
    void nativePipelineFailurePropagatesWithoutEarlyWriteAllocation(
            boolean partitioned, DeleteStatement statement, RuntimeException failure) {
        var fixture = new Fixture(partitioned);
        fixture.metadata.deleteFailure = new UnsupportedOperationException("Metadata DELETE is unsupported");
        fixture.onDispatch = ignored -> { throw failure; };

        assertThatThrownBy(() -> fixture.execute(statement)).isSameAs(failure);

        assertThat(fixture.metadata.overwriteTables).isEmpty();
        fixture.assertNativePending("dispatch");
        assertThat(fixture.tracker.hasWriteContext(PlanExecutionFactoryDeleteTestSupport.QUERY_ID)).isFalse();
    }

    private static Stream<Arguments> metadataFailures() {
        return Stream.of(false, true).flatMap(partitioned -> Stream.of(false, true)
                .flatMap(all -> genuineFailures().map(failure -> Arguments.of(partitioned, all, failure))));
    }

    private static Stream<RuntimeException> genuineFailures() {
        return Stream.of(
                new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR, "Metadata commit failed"),
                new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR, "Wrapped connector failure",
                        new IllegalStateException("Storage failure",
                                new UnsupportedOperationException("Nested cause is not a capability signal"))),
                new ConnectorException(PangrovaErrorCode.CONNECTOR_PERMISSION_DENIED, "Connector access denied"),
                new RuntimeException("Unexpected metadata failure"),
                new SecurityException("DELETE denied"),
                new PangrovaException(PangrovaErrorCode.ACCESS_DENIED, "Policy denied DELETE"));
    }

    private static Stream<Arguments> invalidColumns() {
        return Stream.of(false, true).flatMap(partitioned -> Stream.of(
                equalToSeven("missing_column"),
                new BinaryExpression(equalToSeven("id"), BinaryExpression.Operator.AND,
                        equalToSeven("missing_column")))
                .map(predicate -> Arguments.of(partitioned, predicate)));
    }

    private static Stream<Arguments> overwriteFailures() {
        return Stream.of(false, true).flatMap(partitioned -> Stream.of(
                deleteAll(), deleteWhere(equalToSeven("id")), deleteWhere(arithmeticPredicate()))
                .flatMap(statement -> Stream.of(
                        new UnsupportedOperationException("Overwrite unsupported"),
                        new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR, "Cannot begin overwrite"),
                        new SecurityException("Overwrite denied"))
                        .map(failure -> Arguments.of(partitioned, statement, failure))));
    }
}
