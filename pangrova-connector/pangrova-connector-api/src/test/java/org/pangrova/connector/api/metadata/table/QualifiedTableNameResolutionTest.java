package org.pangrova.connector.api.metadata.table;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QualifiedTableNameResolutionTest {
    @Test
    void fillsOnlyOmittedComponentsWithoutParsingOrFoldingNames() {
        assertThat(QualifiedTableName.resolve(List.of("Table.C"), "Lake.A", "Schema.B"))
                .isEqualTo(new QualifiedTableName("Lake.A", "Schema.B", "Table.C"));
        assertThat(QualifiedTableName.resolve(List.of("Schema.B", "Table.C"), "Lake.A", "ignored"))
                .isEqualTo(new QualifiedTableName("Lake.A", "Schema.B", "Table.C"));
        assertThat(QualifiedTableName.resolve(List.of("Lake.A", "Schema.B", "Table.C"), "ignored", "ignored"))
                .isEqualTo(new QualifiedTableName("Lake.A", "Schema.B", "Table.C"));
        assertThat(QualifiedTableName.resolve(List.of("Rows")))
                .isEqualTo(new QualifiedTableName("default", "default", "Rows"));
    }

    @Test
    void invalidArityIsNotAnAbsentTable() {
        assertThatThrownBy(() -> QualifiedTableName.resolve(List.of(), "c", "s"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QualifiedTableName.resolve(List.of("a", "b", "c", "d"), "c", "s"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
