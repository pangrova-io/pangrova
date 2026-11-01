package org.pangrova.security.mask.memory;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.spi.security.mask.ColumnMask;

class InMemoryEnrichedColumnMaskStoreTest {
    @Test
    void identicalMaskNamesInOtherCatalogsOrSchemasCannotReplaceEachOther() {
        var store = new InMemoryEnrichedColumnMaskStore();
        store.saveColumnMask(mask("cat", "sch", "original"));
        store.saveColumnMask(mask("other", "sch", "other_catalog"));
        store.saveColumnMask(mask("cat", "other", "other_schema"));
        store.saveColumnMask(mask("cat", "sch", "replacement"));
        assertThat(store.getTableMasks("cat", "sch", "orders")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("'replacement'"));
        assertThat(store.getColumnMasks("other", "sch", "orders", "secret")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("'other_catalog'"));
        assertThat(store.getTableMasks("cat", "other", "orders")).singleElement()
                .satisfies(mask -> assertThat(mask.maskExpression()).isEqualTo("'other_schema'"));
        assertThat(store.getTableMasks("cat", "sch", "another")).isEmpty();
    }

    private static ColumnMask mask(String catalog, String schema, String expression) {
        return new ColumnMask(0, "same_name", catalog, schema, "orders", "secret", "'" + expression + "'",
                List.of(), List.of(), true, null);
    }
}
