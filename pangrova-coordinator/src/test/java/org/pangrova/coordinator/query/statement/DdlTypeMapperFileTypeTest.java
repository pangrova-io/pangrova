package org.pangrova.coordinator.query.statement;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.spi.connector.data.type.RowType;

import static org.assertj.core.api.Assertions.assertThat;

class DdlTypeMapperFileTypeTest {

    @Test
    void everyFileStorageVariantMapsToTheSameFiveFieldStruct() {
        RowType external = (RowType) DdlTypeMapper.parameterizedType(DataTypeSpec.FileType.INSTANCE);
        RowType managed = (RowType) DdlTypeMapper.parameterizedType(DataTypeSpec.FileType.MANAGED);

        assertThat(external).isEqualTo(managed);
        assertThat(external.fields()).extracting(field -> field.name())
                .containsExactly("uri", "mime_type", "size", "checksum", "created_at");
    }

    @Test
    void managedModifierAddsStoragePropertyWithoutTouchingTheColumnMarker() {
        Map<String, String> properties = propertiesFor(DataTypeSpec.FileType.MANAGED);

        assertThat(properties)
                .containsEntry("pangrova.file.column.img", "true")
                .containsEntry("pangrova.file.storage.img", "MANAGED");
    }

    @Test
    void externalVariantsOnlyMarkTheColumn() {
        assertThat(propertiesFor(DataTypeSpec.FileType.INSTANCE))
                .containsEntry("pangrova.file.column.img", "true")
                .doesNotContainKey("pangrova.file.storage.img");
    }

    private static Map<String, String> propertiesFor(DataTypeSpec type) {
        Map<String, String> properties = new java.util.HashMap<>();
        DdlTypeMapper.injectStructuralProperties(
                List.of(new CreateTableStatement.ColumnDefinition("img", type)),
                properties);
        return properties;
    }
}
