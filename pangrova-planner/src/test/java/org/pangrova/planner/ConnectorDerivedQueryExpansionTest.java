package org.pangrova.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedQuery;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorDerivedQueryExpansionTest {
    private static final List<ConnectorTableMetadata.Column> VALUES = List.of(
            new ConnectorTableMetadata.Column("key_id", PangrovaType.INTEGER),
            new ConnectorTableMetadata.Column("amount", PangrovaType.BIGINT));

    @Test
    void aggregatesAndJoinsUseExactFrozenInputsAndPhysicalNames() {
        var source = source("orders.中文\"; select", VALUES, "frozen-source");
        var lookup = source("lookup", VALUES, "frozen-lookup");
        var output = List.of(new ConnectorTableMetadata.Column("key_id", PangrovaType.INTEGER),
                new ConnectorTableMetadata.Column("total", PangrovaType.BIGINT));
        var derived = derived("summary", output, "SELECT e.key_id AS key_id, SUM(e.amount) AS total "
                + "FROM entries e LEFT JOIN snapshots s ON e.key_id = s.key_id "
                + "WHERE e.amount > 0 GROUP BY e.key_id", List.of(
                        new ConnectorDerivedQuery.Input("entries", source),
                        new ConnectorDerivedQuery.Input("snapshots", lookup)));
        var plan = planner(derived).planForPolicyApplication("SELECT * FROM summary").plan();
        var scans = scans(plan);
        assertThat(scans).hasSize(2);
        assertThat(scans).anySatisfy(scan -> {
            assertThat(scan.getTable().getQualifiedName()).containsExactly("catalog", "schema", "orders.中文\"; select");
            assertThat(scan.getTable().unwrap(ConnectorReadBinding.class)).isSameAs(source);
        });
        assertThat(scans).anySatisfy(scan ->
                assertThat(scan.getTable().unwrap(ConnectorReadBinding.class)).isSameAs(lookup));
        assertThat(org.apache.calcite.plan.RelOptUtil.toString(plan)).contains("LogicalAggregate", "LogicalJoin");
        assertThat(PlanSplitSourceExtractor.extract(plan)).hasSize(2)
                .allSatisfy(descriptor -> assertThat(descriptor.readBinding()).isPresent());
    }

    @Test
    void repeatedReferencesAndNestedDefinitionsRetainTheSameSelectedSource() {
        var source = source("orders", VALUES, "version-one");
        var inner = derived("inner_summary", VALUES, "SELECT key_id, amount FROM leaf",
                List.of(new ConnectorDerivedQuery.Input("leaf", source)));
        var outer = derived("summary", VALUES, "WITH chosen AS (SELECT * FROM middle) SELECT * FROM chosen",
                List.of(new ConnectorDerivedQuery.Input("middle", inner)));
        var plan = planner(outer).planForPolicyApplication(
                "SELECT a.key_id FROM summary a JOIN summary b ON a.key_id=b.key_id").plan();
        assertThat(scans(plan)).hasSize(2).allSatisfy(scan -> {
            assertThat(scan.getTable().getQualifiedName()).containsExactly("catalog", "schema", "orders");
            assertThat(scan.getTable().unwrap(ConnectorReadBinding.class)).isSameAs(source);
        });
    }

    @Test
    void scalarAndCorrelatedSubqueriesExpandEveryInputBeforePolicyApplication() {
        var source = source("orders", VALUES, "version-one");
        var derived = derived("summary", VALUES, "SELECT * FROM allowed",
                List.of(new ConnectorDerivedQuery.Input("allowed", source)));
        var plan = planner(derived).planForPolicyApplication(
                "SELECT (SELECT SUM(amount) FROM summary) AS total, x.key_id FROM summary x "
                        + "WHERE EXISTS (SELECT 1 FROM summary y WHERE x.key_id=y.key_id)").plan();
        assertThat(scans(plan)).hasSize(3).allSatisfy(scan -> {
            assertThat(scan.getTable().getQualifiedName()).containsExactly("catalog", "schema", "orders");
            assertThat(scan.getTable().unwrap(ConnectorReadBinding.class)).isSameAs(source);
        });
    }

    @Test
    void definitionCannotReadAnAmbientCatalogRelation() {
        var source = source("orders", VALUES, "version-one");
        var derived = derived("summary", VALUES, "SELECT * FROM catalog.schema.ambient",
                List.of(new ConnectorDerivedQuery.Input("allowed", source)));
        var planner = planner(derived);
        assertThatThrownBy(() -> planner.planForPolicyApplication("SELECT * FROM summary"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("catalog");
    }

    @Test
    void emptyPredicatesDoNotEraseSourcesBeforeAuthorization() {
        var source = source("orders", VALUES, "version-one");
        var inputs = List.of(new ConnectorDerivedQuery.Input("allowed", source));
        var definition = derived("summary", VALUES, "SELECT * FROM allowed WHERE FALSE", inputs);
        assertThat(planner(definition).planForPolicyApplication("SELECT * FROM summary")
                .toPlannedQuery().tableReferences()).containsExactly(List.of("catalog", "schema", "orders"));
        var ordinary = derived("summary", VALUES, "SELECT * FROM allowed", inputs);
        assertThat(planner(ordinary).planForPolicyApplication("SELECT * FROM summary WHERE FALSE")
                .toPlannedQuery().tableReferences()).containsExactly(List.of("catalog", "schema", "orders"));
    }

    @Test
    void nonnullableCountFitsItsNullableDeclaredColumnWithoutNumericCoercion() {
        var source = source("orders", VALUES, "version-one");
        var columns = List.of(new ConnectorTableMetadata.Column("total", PangrovaType.BIGINT));
        var derived = derived("summary", columns, "SELECT COUNT(*) AS total FROM allowed",
                List.of(new ConnectorDerivedQuery.Input("allowed", source)));
        var plan = planner(derived).planForPolicyApplication("SELECT * FROM summary").plan();
        assertThat(plan.getRowType().getFieldList().get(0).getType().getSqlTypeName())
                .isEqualTo(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
        assertThat(plan.getRowType().getFieldList().get(0).getType().isNullable()).isTrue();
        assertThat(scans(plan)).singleElement().satisfies(scan ->
                assertThat(scan.getTable().unwrap(ConnectorReadBinding.class)).isSameAs(source));
    }

    @Test
    void definitionCannotDiscardAllDeclaredAuthorityAndInventConstantRows() {
        var source = source("orders", VALUES, "version-one");
        var columns = List.of(new ConnectorTableMetadata.Column("total", PangrovaType.BIGINT));
        var derived = derived("summary", columns, "SELECT CAST(7 AS BIGINT) AS total",
                List.of(new ConnectorDerivedQuery.Input("allowed", source)));
        assertThatThrownBy(() -> planner(derived).planForPolicyApplication("SELECT * FROM summary"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("frozen source for authorization");
    }

    @Test
    void mutationsAndOutputTypeDriftFailBeforePolicyOrDispatch() {
        var source = source("orders", VALUES, "version-one");
        var inputs = List.of(new ConnectorDerivedQuery.Input("allowed", source));
        var mutation = derived("summary", VALUES, "INSERT INTO allowed VALUES (1, 2)", inputs);
        assertThatThrownBy(() -> planner(mutation).planForPolicyApplication("SELECT * FROM summary"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("read-only query");
        var drift = derived("summary", VALUES,
                "SELECT key_id, CAST(amount AS INTEGER) AS amount FROM allowed", inputs);
        assertThatThrownBy(() -> planner(drift).planForPolicyApplication("SELECT * FROM summary"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("frozen declaration");
    }

    @Test
    void recursiveProviderDefinitionIsRejectedWithoutLeakingExpansionState() {
        var self = new AtomicReference<ConnectorReadBinding>();
        var handle = new ConnectorDerivedTableHandle() {
            @Override public ConnectorDerivedQuery derivedQuery() {
                return new ConnectorDerivedQuery("SELECT * FROM recursive_input",
                        List.of(new ConnectorDerivedQuery.Input("recursive_input", self.get())));
            }
        };
        self.set(binding("summary", VALUES, "recursive-source", handle));
        assertThatThrownBy(() -> planner(self.get()).planForPolicyApplication("SELECT * FROM summary"))
                .isInstanceOf(PlannerException.class).hasStackTraceContaining("Recursive or excessively nested");
    }

    private static PangrovaPlanner planner(ConnectorReadBinding derived) {
        var root = CalciteSchema.createRootSchema(false, false);
        var catalog = root.add("catalog", new org.apache.calcite.schema.impl.AbstractSchema());
        var schema = catalog.add("schema", new org.apache.calcite.schema.impl.AbstractSchema());
        schema.add(derived.declaration().metadata().table().table(), new BoundTable(derived));
        // It exists in the outer catalog, but definition scope must never expose it.
        schema.add("ambient", new BoundTable(source("ambient", VALUES, "live-ambient")));
        return new PangrovaPlanner(root, List.of("catalog", "schema"));
    }

    private static ConnectorReadBinding derived(String name, List<ConnectorTableMetadata.Column> columns,
            String sql, List<ConnectorDerivedQuery.Input> inputs) {
        return binding(name, columns, "derived-" + name, new DerivedHandle(new ConnectorDerivedQuery(sql, inputs)));
    }

    private static ConnectorReadBinding source(String name, List<ConnectorTableMetadata.Column> columns,
            String identity) {
        return binding(name, columns, identity, new ConnectorTableHandle() {});
    }

    private static ConnectorReadBinding binding(String name, List<ConnectorTableMetadata.Column> columns,
            String identity, ConnectorTableHandle handle) {
        var metadata = new ConnectorTableMetadata(new SchemaTableName("schema", name),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of());
        return new ConnectorReadBinding(handle,
                new ConnectorTableReadMetadata(metadata, ConnectorTableReadMetadata.DeclarationEvidence.EXACT),
                TableStatistics.UNKNOWN, ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.empty(), identity, java.util.Optional.empty());
    }

    private static List<TableScan> scans(RelNode root) {
        var scans = new ArrayList<TableScan>();
        new RelVisitor() {
            @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan scan) scans.add(scan);
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return scans;
    }

    private record DerivedHandle(ConnectorDerivedQuery derivedQuery) implements ConnectorDerivedTableHandle {}

    private static final class BoundTable extends AbstractTable {
        private final ConnectorReadBinding binding;
        BoundTable(ConnectorReadBinding binding) { this.binding = binding; }
        @Override public RelDataType getRowType(RelDataTypeFactory factory) {
            return ConnectorColumnTypeMapper.rowType(binding.declaration().metadata().typedColumns(), factory);
        }
        @Override public <T> T unwrap(Class<T> type) {
            return type.isInstance(binding) ? type.cast(binding) : super.unwrap(type);
        }
    }
}
