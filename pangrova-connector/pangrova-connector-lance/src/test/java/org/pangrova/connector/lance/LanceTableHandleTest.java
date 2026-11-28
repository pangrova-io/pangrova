package org.pangrova.connector.lance;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.types.enums.PangrovaType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceTableHandleTest {
    @Test
    void acceptsTheConnectorsCanonicalStringVersionAndRejectsOtherTokens() {
        var column = LanceColumn.from(new org.pangrova.connector.api.metadata.table.ConnectorTableMetadata.Column(
                "id", PangrovaType.BIGINT));
        var current = new LanceTableHandle(
                "catalog", "tenant", new SchemaTableName("db", "items"), "table-id",
                "file:///warehouse/items", List.of(column), Map.of(), new byte[32], 1, 1, 1);

        var selected = (LanceTableHandle) current.withTimeTravelSnapshot("7");

        assertThat(selected.version()).isEqualTo(7);
        for (String token : List.of("opaque", "0", "-1", "07")) {
            assertThatThrownBy(() -> current.withTimeTravelSnapshot(token))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
