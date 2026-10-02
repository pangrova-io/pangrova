package org.pangrova.testing;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.TestDataGenerator.ColumnDef;
import org.pangrova.testing.TestDataGenerator.ColumnType;
import org.pangrova.testing.TestDataGenerator.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDataGeneratorTest {

    @Test
    void schemaFreezesOrderedColumnsAndRejectsAccessorMutation() {
        var id = new ColumnDef("id", ColumnType.LONG);
        var name = new ColumnDef("name", ColumnType.STRING);
        var columns = new ArrayList<>(List.of(id, name));
        var schema = new TableSchema(columns);
        columns.clear();
        assertThat(schema.columns()).containsExactly(id, name);
        assertThatThrownBy(() -> schema.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void schemaRejectsNullListAndNullColumns() {
        assertThatThrownBy(() -> new TableSchema(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new TableSchema(Arrays.asList((ColumnDef) null)))
                .isInstanceOf(NullPointerException.class);
        assertThat(new TableSchema(List.of()).columns()).isEmpty();
    }

    @Test
    void catalogSchemasAndTheirColumnsCannotBeModified() {
        var schemas = TestDataGenerator.getSchemas();
        assertThatThrownBy(schemas::clear).isInstanceOf(UnsupportedOperationException.class);
        for (TableSchema schema : schemas.values()) {
            assertThatThrownBy(() -> schema.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
        }
        assertThat(TestDataGenerator.getSchemas()).hasSize(8);
    }

    @Test
    void sameSeedAndGenerationOrderReproduceEveryTableExactly() {
        var first = new TestDataGenerator(8675309L);
        var second = new TestDataGenerator(8675309L);
        for (String table : TestDataGenerator.getSchemas().keySet().stream().sorted().toList()) {
            assertThat(first.generateTable(table, 100)).as("seeded table %s", table)
                    .isEqualTo(second.generateTable(table, 100));
        }
    }

    @Test
    void defaultSeedIsExplicitFortyTwoAndDifferentSeedsChangeData() {
        assertThat(new TestDataGenerator().generateTable("orders", 100))
                .isEqualTo(new TestDataGenerator(42L).generateTable("orders", 100))
                .isNotEqualTo(new TestDataGenerator(43L).generateTable("orders", 100));
    }

    @Test
    void invalidGenerationInputsAreRejected() {
        var generator = new TestDataGenerator(42L);
        assertThatThrownBy(() -> generator.generateTable(null, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> generator.generateTable("unknown", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> generator.generateTable("orders", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> generator.generateTable("orders", -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
