package org.pangrova.connector.paimon.scan;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Read schema options remain stable when callers retain mutable input maps. */
class PaimonReadSchemaTest {
    private static final String VARIANT_PROPERTY = PaimonTypeMapper.VARIANT_COLUMN_KEY_PREFIX + "payload";

    @Test
    void inputOptionsCannotChangeTheConstructedSchema() {
        var options = new LinkedHashMap<>(Map.of(VARIANT_PROPERTY, "true"));
        var schema = new PaimonReadSchema(payloadSchema(), options);

        options.clear();

        assertThat(schema.options()).containsExactlyEntriesOf(Map.of(VARIANT_PROPERTY, "true"));
    }

    @Test
    void optionsAccessorDoesNotPermitMutation() {
        var schema = new PaimonReadSchema(payloadSchema(),
                new LinkedHashMap<>(Map.of(VARIANT_PROPERTY, "true")));

        assertThatThrownBy(() -> schema.options().remove(VARIANT_PROPERTY))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(schema.options()).containsExactlyEntriesOf(Map.of(VARIANT_PROPERTY, "true"));
    }

    @Test
    void realSdkVarcharVariantHandleKeepsItsDeclarationAfterInputOptionsChange() {
        var rowType = payloadSchema();
        var options = new LinkedHashMap<>(Map.of(VARIANT_PROPERTY, "true"));
        var schema = new PaimonReadSchema(rowType, options);
        var original = (PaimonColumnHandle) schema.columnHandles().get("payload");
        assertThat(original).isEqualTo(new PaimonColumnHandle(7, "payload", PangrovaType.VARIANT));
        assertThat(new PaimonReadSchema(rowType, Map.of()).columnHandles().get("payload"))
                .isEqualTo(new PaimonColumnHandle(7, "payload", PangrovaType.VARCHAR));

        options.remove(VARIANT_PROPERTY);

        assertThat(schema.columnHandles().get("payload")).isEqualTo(original);
        assertThat(schema.rowType()).isSameAs(rowType);
    }

    private static RowType payloadSchema() {
        return DataTypes.ROW(DataTypes.FIELD(7, "payload", DataTypes.STRING()));
    }
}
