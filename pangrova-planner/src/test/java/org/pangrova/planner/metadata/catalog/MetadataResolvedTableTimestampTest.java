package org.pangrova.planner.metadata.catalog;

import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataResolvedTableTimestampTest {

    @Test
    void preservesMicrosecondAndNanosecondConnectorSchemas() {
        var columns = List.of(
                new ConnectorTableMetadata.Column("micros", PangrovaType.TIMESTAMP,
                        OptionalInt.empty(), null),
                new ConnectorTableMetadata.Column("nanos", PangrovaType.TIMESTAMP_NS,
                        OptionalInt.empty(), null),
                new ConnectorTableMetadata.Column("tz", PangrovaType.TIMESTAMP_TZ,
                        OptionalInt.empty(), null));
        var metadata = new ConnectorTableMetadata(
                new org.pangrova.connector.api.metadata.table.SchemaTableName("schema", "timestamps"),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, java.util.Map.of());
        var table = new MetadataResolvedTable(metadata);
        var factory = new PangrovaPlanner().plan("SELECT 1").plan()
                .getCluster().getTypeFactory();

        var row = table.getRowType(factory);

        assertThat(row.getFieldList()).extracting(field -> field.getType().getPrecision())
                .containsExactly(6, 9, 6);
    }
}
