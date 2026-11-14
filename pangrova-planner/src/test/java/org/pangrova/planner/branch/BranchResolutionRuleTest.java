package org.pangrova.planner.branch;

import org.pangrova.parser.api.ast.AllColumns;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.JoinClause;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.TimeTravelSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BranchResolutionRuleTest {

    private BranchRoutingContext context;
    private BranchResolutionRule rule;

    @BeforeEach
    void setUp() {
        context = new BranchRoutingContext();
        rule = new BranchResolutionRule(context);
    }

    @Nested
    @DisplayName("No active routing")
    class NoRouting {

        @Test
        @DisplayName("Returns original statement when no routing is active")
        void noRoutingReturnsOriginal() {
            Query query = selectAllFrom("catalog", "schema", "orders");
            Statement result = rule.resolve(query);
            assertThat(result).isSameAs(query);
        }
    }

    @Nested
    @DisplayName("Single table routing")
    class SingleTableRouting {

        @Test
        @DisplayName("Injects branch spec for exact FQN match")
        void exactFqnMatch() {
            context.setRouting("catalog.schema.orders", "feature_branch");
            Query query = selectAllFrom("catalog", "schema", "orders");

            Query resolved = (Query) rule.resolve(query);

            TableRef resolvedTable = resolved.from().get(0);
            assertThat(resolvedTable.timeTravelSpec()).isPresent();
            assertThat(resolvedTable.timeTravelSpec().get().type())
                    .isEqualTo(TimeTravelSpec.Type.BRANCH);
            assertThat(resolvedTable.timeTravelSpec().get().refName())
                    .isEqualTo("feature_branch");
        }

        @Test
        @DisplayName("Matches case-insensitively")
        void caseInsensitiveMatch() {
            context.setRouting("CATALOG.SCHEMA.ORDERS", "dev_branch");
            Query query = selectAllFrom("catalog", "schema", "orders");

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec()).isPresent();
            assertThat(resolved.from().get(0).timeTravelSpec().get().refName())
                    .isEqualTo("dev_branch");
        }

        @Test
        @DisplayName("Partial qualification: routing for FQN matches short reference")
        void partialQualificationShortRef() {
            context.setRouting("catalog.schema.orders", "experiment");
            Query query = selectAllFrom("orders");

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec()).isPresent();
            assertThat(resolved.from().get(0).timeTravelSpec().get().refName())
                    .isEqualTo("experiment");
        }

        @Test
        @DisplayName("Does not inject when table has no matching routing")
        void noMatchingRouting() {
            context.setRouting("catalog.schema.customers", "branch_x");
            Query query = selectAllFrom("catalog", "schema", "orders");

            Statement result = rule.resolve(query);

            Query resolved = (Query) result;
            assertThat(resolved.from().get(0).timeTravelSpec()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Explicit time travel precedence")
    class ExplicitTimeTravelPrecedence {

        @Test
        @DisplayName("Does not override explicit VERSION AS OF BRANCH")
        void explicitBranchNotOverridden() {
            context.setRouting("catalog.schema.orders", "session_branch");
            TableRef tableWithBranch = new TableRef(
                    Identifier.of("catalog", "schema", "orders"),
                    Optional.of(TimeTravelSpec.branch("explicit_branch")),
                    Optional.empty());
            Query query = queryWithFrom(List.of(tableWithBranch));

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec().get().refName())
                    .isEqualTo("explicit_branch");
        }

        @Test
        @DisplayName("Does not override explicit VERSION AS OF TAG")
        void explicitTagNotOverridden() {
            context.setRouting("catalog.schema.orders", "session_branch");
            TableRef tableWithTag = new TableRef(
                    Identifier.of("catalog", "schema", "orders"),
                    Optional.of(TimeTravelSpec.tag("v1.0")),
                    Optional.empty());
            Query query = queryWithFrom(List.of(tableWithTag));

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec().get().type())
                    .isEqualTo(TimeTravelSpec.Type.TAG);
            assertThat(resolved.from().get(0).timeTravelSpec().get().refName())
                    .isEqualTo("v1.0");
        }
    }

    @Nested
    @DisplayName("Per-table routing")
    class PerTableRouting {

        @Test
        @DisplayName("Different tables route to different branches")
        void differentBranchesPerTable() {
            context.setRouting("catalog.schema.orders", "branch_a");
            context.setRouting("catalog.schema.customers", "branch_b");

            TableRef ordersRef = new TableRef(Identifier.of("catalog", "schema", "orders"));
            TableRef customersRef = new TableRef(Identifier.of("catalog", "schema", "customers"));
            Query query = queryWithFrom(List.of(ordersRef, customersRef));

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec().get().refName())
                    .isEqualTo("branch_a");
            assertThat(resolved.from().get(1).timeTravelSpec().get().refName())
                    .isEqualTo("branch_b");
        }

        @Test
        @DisplayName("Only routed tables get branch spec, others unchanged")
        void mixedRoutedAndUnrouted() {
            context.setRouting("catalog.schema.orders", "branch_a");

            TableRef ordersRef = new TableRef(Identifier.of("catalog", "schema", "orders"));
            TableRef productsRef = new TableRef(Identifier.of("catalog", "schema", "products"));
            Query query = queryWithFrom(List.of(ordersRef, productsRef));

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.from().get(0).timeTravelSpec()).isPresent();
            assertThat(resolved.from().get(1).timeTravelSpec()).isEmpty();
        }
    }

    @Nested
    @DisplayName("JOIN clause routing")
    class JoinRouting {

        @Test
        @DisplayName("Resolves branch in JOIN right table")
        void joinRightTableResolved() {
            context.setRouting("catalog.schema.items", "join_branch");

            TableRef leftRef = new TableRef(Identifier.of("catalog", "schema", "orders"));
            TableRef rightRef = new TableRef(Identifier.of("catalog", "schema", "items"));
            JoinClause join = new JoinClause(
                    JoinClause.JoinType.INNER, rightRef, Optional.of(Literal.ofBoolean(true)));
            Query query = new Query(
                    false,
                    List.of(new AllColumns()),
                    List.of(leftRef),
                    List.of(join),
                    Optional.empty(),
                    List.of(),
                    Optional.empty(),
                    List.of(),
                    Optional.empty());

            Query resolved = (Query) rule.resolve(query);

            assertThat(resolved.joins().get(0).right().timeTravelSpec()).isPresent();
            assertThat(resolved.joins().get(0).right().timeTravelSpec().get().refName())
                    .isEqualTo("join_branch");
        }
    }

    @Nested
    @DisplayName("USE BRANCH MAIN removes routing")
    class MainBranchRemoval {

        @Test
        @DisplayName("Setting 'main' removes routing for table")
        void mainRemovesRouting() {
            context.setRouting("catalog.schema.orders", "feature_branch");
            context.setRouting("catalog.schema.orders", "main");

            Query query = selectAllFrom("catalog", "schema", "orders");
            Statement result = rule.resolve(query);

            // No routing active, returns original
            assertThat(result).isSameAs(query);
        }
    }

    // ── Test helpers ──

    private static Query selectAllFrom(String... tableParts) {
        TableRef ref = new TableRef(Identifier.of(tableParts));
        return queryWithFrom(List.of(ref));
    }

    private static Query queryWithFrom(List<TableRef> from) {
        return new Query(
                false,
                List.of(new AllColumns()),
                from,
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty());
    }
}
