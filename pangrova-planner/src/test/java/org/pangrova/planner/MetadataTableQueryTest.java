package org.pangrova.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.MetadataTableKind;
import org.pangrova.spi.connector.metadata.MetadataTableListing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTableQueryTest {
    @Test
    void structuredNamesRetainDotsUnicodeQuotesAndReservedWords() {
        String base = "Orders.中文\"; select";
        var result = planner(base).planForPolicyApplication(new MetadataTableQuery(
                List.of("lake", "db", base), new MetadataTableListing(MetadataTableKind.REFS)));
        var scans = new ArrayList<TableScan>();
        new RelVisitor() {
            @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan scan) scans.add(scan);
                super.visit(node, ordinal, parent);
            }
        }.go(result.plan());
        assertThat(scans).singleElement().satisfies(scan -> assertThat(scan.getTable().getQualifiedName())
                .containsExactly("lake", "db", base + "$refs"));
    }

    @Test
    void listingFilterIsBoundAsDataAndRetainsItsScanBeforeAuthorization() {
        String value = "TAG' OR TRUE --";
        var plan = planner("orders").planForPolicyApplication(new MetadataTableQuery(
                List.of("lake", "db", "orders"),
                new MetadataTableListing(MetadataTableKind.REFS, Map.of("type", value)))).plan();
        var filters = new ArrayList<Filter>();
        new RelVisitor() {
            @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof Filter filter) filters.add(filter);
                super.visit(node, ordinal, parent);
            }
        }.go(plan);
        assertThat(filters).singleElement().satisfies(filter -> {
            var condition = (RexCall) filter.getCondition();
            assertThat(((RexLiteral) condition.getOperands().get(1)).getValueAs(String.class)).isEqualTo(value);
        });
        assertThat(RelOptUtil.toString(plan)).contains("TableScan");
    }

    @Test
    void invalidTableIdentityIsRejectedInsteadOfSilentlyUsingDefaults() {
        assertThatThrownBy(() -> new MetadataTableQuery(List.of(), new MetadataTableListing(MetadataTableKind.REFS)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PangrovaPlanner planner(String name) {
        var root = CalciteSchema.createRootSchema(false, false);
        root.plus().add("lake", new AbstractSchema()).add("db", new AbstractSchema())
                .add(name + "$refs", new AbstractTable() {
                    @Override public RelDataType getRowType(RelDataTypeFactory factory) {
                        return factory.builder().add("name", SqlTypeName.VARCHAR)
                                .add("type", SqlTypeName.VARCHAR).add("snapshot_id", SqlTypeName.BIGINT).build();
                    }
                });
        return new PangrovaPlanner(root);
    }
}
