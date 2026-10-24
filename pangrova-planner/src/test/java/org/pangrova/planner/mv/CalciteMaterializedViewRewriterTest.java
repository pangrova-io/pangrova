package org.pangrova.planner.mv;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannerContext;

class CalciteMaterializedViewRewriterTest {

    private static final String DEFINITION =
            "SELECT id, SUM(amount) AS total FROM orders GROUP BY id";
    private static final Instant NOW = Instant.now();

    private CalciteSchema schema;
    private MutableCatalog catalog;
    private PangrovaPlanner planner;

    @BeforeEach
    void setUp() {
        schema = CalciteSchema.createRootSchema(false);
        var db = schema.plus().add("test", new AbstractSchema())
                .add("db", new AbstractSchema());
        db.add("orders", table("id", "amount"));
        db.add("__pg_mat_orders", table("id", "total"));
        catalog = new MutableCatalog();
        planner = planner(catalog);
    }

    @Test
    void freshEligibleViewReplacesTheAggregateWithThePublishedGeneration() {
        catalog.views = List.of(view(DEFINITION, NOW));

        RelNode plan = planner.plan("SELECT id, SUM(amount) AS total FROM orders GROUP BY id")
                .plan();

        assertThat(plan.explain().toLowerCase())
                .contains("__pg_mat_orders")
                .doesNotContain("logicalaggregate");
        MaterializedViewRewriteDecision decision = planner.lastMaterializedViewRewrite();
        assertThat(decision.applied()).isTrue();
        assertThat(decision.views()).containsExactly("test.db.orders_mv");
        assertThat(decision.describe()).contains("applied");
    }

    @Test
    void staleViewLeavesTheQueryOnItsSources() {
        catalog.views = List.of(view(DEFINITION, NOW.minus(Duration.ofHours(2))));

        RelNode plan = planner.plan("SELECT id, SUM(amount) AS total FROM orders GROUP BY id")
                .plan();

        assertThat(plan.explain().toLowerCase())
                .contains("table=[[test, db, orders]]")
                .doesNotContain("__pg_mat_orders");
        assertThat(planner.lastMaterializedViewRewrite().applied()).isFalse();
        assertThat(planner.lastMaterializedViewRewrite().reason()).isEqualTo("stale");
    }

    @Test
    void nonMatchingQueryLeavesTheViewUnused() {
        catalog.views = List.of(view(DEFINITION, NOW));

        RelNode plan = planner.plan("SELECT id, amount FROM orders WHERE amount > 5")
                .plan();

        assertThat(plan.explain().toLowerCase()).doesNotContain("__pg_mat_orders");
        assertThat(planner.lastMaterializedViewRewrite().reason())
                .isEqualTo("no_equivalent_substitution");
    }

    @Test
    void queryWithoutPublishedCandidateReportsThatReason() {
        RelNode plan = planner.plan("SELECT id, amount FROM orders").plan();

        assertThat(plan.explain().toLowerCase()).doesNotContain("__pg_mat_orders");
        assertThat(planner.lastMaterializedViewRewrite().reason())
                .isEqualTo("no_published_candidate");
    }

    @Test
    void definitionPlanningNeverRewritesThroughAnotherView() {
        catalog.views = List.of(view(DEFINITION, NOW));

        RelNode definition = planner.planDefinition(DEFINITION).plan();

        assertThat(definition.explain().toLowerCase())
                .contains("table=[[test, db, orders]]")
                .doesNotContain("__pg_mat_orders");
    }

    @Test
    void inlineProjectionViewIsEligible() {
        assertThat(MaterializedViewPlanEligibility.ineligibleReason(
                planner.planDefinition("SELECT id, amount FROM orders").plan())).isEmpty();
    }

    @Test
    void distinctAggregateIsRejected() {
        assertThat(MaterializedViewPlanEligibility.ineligibleReason(
                planner.planDefinition("SELECT id, COUNT(DISTINCT amount) FROM orders GROUP BY id")
                        .plan()))
                .hasValueSatisfying(reason -> assertThat(reason).contains("DISTINCT"));
    }

    @Test
    void sortIsRejected() {
        assertThat(MaterializedViewPlanEligibility.ineligibleReason(
                planner.planDefinition("SELECT id FROM orders ORDER BY id").plan()))
                .hasValueSatisfying(reason -> assertThat(reason)
                        .contains("unsupported relational operator"));
    }

    @Test
    void outerJoinIsRejected() {
        assertThat(MaterializedViewPlanEligibility.ineligibleReason(
                planner.planDefinition("SELECT o.id FROM orders o "
                        + "LEFT JOIN __pg_mat_orders m ON o.id = m.id").plan()))
                .hasValueSatisfying(reason -> assertThat(reason).contains("only INNER"));
    }

    private PangrovaPlanner planner(MaterializedViewRewriteCatalog views) {
        return new PangrovaPlanner(schema, List.of("test", "db"),
                new PlannerContext(null, views, null, null, null, null, null, null, null));
    }

    private static MaterializedViewRewriteCatalog.PublishedView view(String definition,
            Instant publishedAt) {
        return new MaterializedViewRewriteCatalog.PublishedView("test", "db", "orders_mv",
                definition, "test", "db", "__pg_mat_orders", 1, publishedAt,
                Duration.ofMinutes(30));
    }

    private static AbstractTable table(String... columns) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                var builder = factory.builder();
                for (String column : columns) {
                    builder.add(column, SqlTypeName.BIGINT).nullable(true);
                }
                return builder.build();
            }
        };
    }

    private static final class MutableCatalog implements MaterializedViewRewriteCatalog {

        private List<PublishedView> views = List.of();

        @Override
        public List<PublishedView> candidates(Set<String> referencedTables) {
            return views;
        }
    }
}
