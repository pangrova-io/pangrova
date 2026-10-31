package org.pangrova.coordinator.query.execution.dispatch;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.Fixture;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.expression.BinaryExpression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.arithmeticPredicate;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.assertDmlResult;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.deleteAll;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.deleteWhere;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.equalToSeven;

/** Every DELETE uses the protected Native survivor-write and durable commit boundary. */
class PlanExecutionFactoryDeleteTest {

    @ParameterizedTest(name = "supported ALL, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void deleteAllDefersAllocationAndDispatchesOriginalStatement(boolean partitioned) {
        var fixture = new Fixture(partitioned);

        StatementResult result = fixture.execute(deleteAll());

        assertDmlResult(result);
        fixture.assertNativePending("dispatch");
        assertThat(fixture.metadata.filters).isEmpty();
    }

    @ParameterizedTest(name = "supported simple predicate, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void simpleFilterCannotBypassNativeAuthorizationAndCommit(boolean partitioned) {
        var fixture = new Fixture(partitioned);

        StatementResult result = fixture.execute(deleteWhere(equalToSeven("id")));

        assertDmlResult(result);
        fixture.assertNativePending("dispatch");
        assertThat(fixture.metadata.filters).isEmpty();
    }

    @ParameterizedTest(name = "case-insensitive column validation, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void predicateRetainsItsOriginalColumnSpellingForPlannerValidation(boolean partitioned) {
        var fixture = new Fixture(partitioned);

        StatementResult result = fixture.execute(deleteWhere(equalToSeven("ID")));

        assertDmlResult(result);
        fixture.assertNativePending("dispatch");
        assertThat(fixture.metadata.filters).isEmpty();
    }

    @ParameterizedTest(name = "unsupported ALL, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void connectorDeleteCapabilityDoesNotChangeCanonicalDeleteAllRouting(boolean partitioned) {
        var fixture = new Fixture(partitioned);
        var original = deleteAll();
        fixture.metadata.deleteFailure = new UnsupportedOperationException("Metadata DELETE is unsupported");
        fixture.onDispatch = statement -> {
            assertThat(fixture.events).containsExactly("dispatch");
            assertThat(statement).isSameAs(original);
        };

        StatementResult result = fixture.execute(original);

        assertDmlResult(result);
        assertThat(fixture.metadata.filters).isEmpty();
        assertThat(fixture.metadata.table.partitionKeys())
                .containsExactlyElementsOf(partitioned ? java.util.List.of("region") : java.util.List.of());
        fixture.assertNativePending("dispatch");
        assertThat(original.where()).isEmpty();
    }

    @ParameterizedTest(name = "unsupported simple predicate, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void unsupportedSimpleFilterDispatchesOriginalDeleteWithoutEarlyAllocation(boolean partitioned) {
        var fixture = new Fixture(partitioned);
        var original = deleteWhere(equalToSeven("id"));
        fixture.metadata.deleteFailure = new UnsupportedOperationException("Predicate DELETE is unsupported");
        fixture.onDispatch = statement -> assertThat(statement).isSameAs(original);

        StatementResult result = fixture.execute(original);

        assertDmlResult(result);
        assertThat(fixture.metadata.filters).isEmpty();
        fixture.assertNativePending("dispatch");
    }

    @ParameterizedTest(name = "nonconvertible predicate, partitioned={0}, predicate={1}")
    @MethodSource("nonconvertiblePredicates")
    void nonconvertiblePredicateDispatchesOriginalDeleteWithoutMetadataDelete(
            boolean partitioned, Expression predicate) {
        var fixture = new Fixture(partitioned);
        var original = deleteWhere(predicate);
        fixture.onDispatch = statement -> assertThat(statement).isSameAs(original);

        StatementResult result = fixture.execute(original);

        assertDmlResult(result);
        assertThat(fixture.metadata.filters).isEmpty();
        fixture.assertNativePending("dispatch");
    }

    private static Stream<Arguments> nonconvertiblePredicates() {
        return Stream.of(false, true).flatMap(partitioned -> Stream.of(
                arithmeticPredicate(),
                new BinaryExpression(Identifier.of("id"), BinaryExpression.Operator.GREATER_THAN,
                        Identifier.of("id")))
                .map(predicate -> Arguments.of(partitioned, predicate)));
    }

}
