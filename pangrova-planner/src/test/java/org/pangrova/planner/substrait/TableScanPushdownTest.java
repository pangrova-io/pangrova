package org.pangrova.planner.substrait;

import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlanResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Substrait plan structure produced by PlanNodeConverter for
 * table scan queries with projections and filters.
 */
class TableScanPushdownTest {

    private PangrovaPlanner planner;
    private SubstraitConverter converter;

    @BeforeEach
    void setUp() {
        planner = buildPlannerWithTable();
        converter = new SubstraitConverter();
    }

    @Test
    void selectSubsetColumnsProducesProjectOverNamedScan() {
        PlanResult result = planner.plan("SELECT A, B FROM T");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        assertThat(root).isInstanceOf(io.substrait.relation.Project.class);
        io.substrait.relation.Project project = (io.substrait.relation.Project) root;
        assertThat(project.getExpressions()).hasSize(2);
    }

    @Test
    void selectWithFilterProducesFilterRelOverNamedScan() {
        PlanResult result = planner.plan("SELECT A FROM T WHERE B > 10");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        assertThat(findFilterRel(root)).isTrue();
    }

    @Test
    void selectWithFilterProducesProjectAndFilterOverNamedScan() {
        PlanResult result = planner.plan("SELECT A, B FROM T WHERE C > 10");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        assertThat(scan.getInitialSchema().names()).containsExactly("A", "B", "C", "D");
        assertThat(findFilterRel(root)).isTrue();
        assertThat(root).isInstanceOf(io.substrait.relation.Project.class);
    }

    @Test
    void selectAllColumnsProducesNoRemap() {
        PlanResult result = planner.plan("SELECT A, B, C, D FROM T");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        // All columns selected — no remap needed.
        assertThat(scan.getRemap()).isEmpty();
    }

    @Test
    void filterOnlyWithoutProjectionProducesFilterRelOverNamedScan() {
        PlanResult result = planner.plan("SELECT * FROM T WHERE A > 5");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        assertThat(findFilterRel(root)).isTrue();
    }

    @Test
    void namedScanRetainsFullSchemaRegardlessOfProjection() {
        PlanResult result = planner.plan("SELECT A FROM T");
        Plan plan = converter.convert(result.plan());

        Rel root = plan.getRoots().get(0).getInput();
        NamedScan scan = findNamedScan(root);
        assertThat(scan).isNotNull();
        // initialSchema should always contain the full table schema.
        assertThat(scan.getInitialSchema().names()).containsExactly("A", "B", "C", "D");
    }

    private static NamedScan findNamedScan(Rel rel) {
        if (rel instanceof NamedScan ns) {
            return ns;
        }
        for (Rel input : rel.getInputs()) {
            NamedScan found = findNamedScan(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static boolean findFilterRel(Rel rel) {
        if (rel instanceof io.substrait.relation.Filter) {
            return true;
        }
        for (Rel input : rel.getInputs()) {
            if (findFilterRel(input)) {
                return true;
            }
        }
        return false;
    }

    private static PangrovaPlanner buildPlannerWithTable() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("T", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                        .add("A", SqlTypeName.INTEGER)
                        .add("B", SqlTypeName.INTEGER)
                        .add("C", SqlTypeName.INTEGER)
                        .add("D", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        });
        return new PangrovaPlanner(rootSchema);
    }
}
