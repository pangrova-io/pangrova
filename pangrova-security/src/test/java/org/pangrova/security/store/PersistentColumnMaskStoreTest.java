package org.pangrova.security.store;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.testinfra.SharedSecurityPostgres;
import org.pangrova.testing.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Table-scoped policy enumeration must include every active column and survive reopening. */
class PersistentColumnMaskStoreTest extends ContainerTestBase {
    @Test
    void loadsAllActiveColumnsOnlyFromTheSelectedTableAndKeepsColumnLookupExact() throws java.sql.SQLException {
        var dataSource = SharedSecurityPostgres.isolatedDataSource();
        var store = new PersistentColumnMaskStore(dataSource);
        store.initSchema();
        store.saveColumnMask(mask("first", "cat", "sch", "orders", "region", true));
        store.saveColumnMask(mask("second", "cat", "sch", "orders", "secret", true));
        store.saveColumnMask(mask("disabled", "cat", "sch", "orders", "disabled", false));
        store.saveColumnMask(mask("other_cat", "other", "sch", "orders", "region", true));
        store.saveColumnMask(mask("other_schema", "cat", "other", "orders", "region", true));
        store.saveColumnMask(mask("other_table", "cat", "sch", "another", "region", true));
        assertThat(store.getTableMasks("cat", "sch", "orders")).extracting(ColumnMask::columnName)
                .containsExactly("region", "secret");
        var first = store.getColumnMasks("cat", "sch", "orders", "region");
        assertThat(first).singleElement().satisfies(mask -> assertThat(mask.maskName()).isEqualTo("first"));
        var reopened = new PersistentColumnMaskStore(dataSource);
        assertThat(reopened.getTableMasks("cat", "sch", "orders")).hasSize(2);
        reopened.deleteMask(first.get(0).maskId());
        assertThat(store.getTableMasks("cat", "sch", "orders")).singleElement()
                .satisfies(mask -> assertThat(mask.columnName()).isEqualTo("secret"));
    }

    private static ColumnMask mask(String name, String catalog, String schema, String table, String column,
            boolean enabled) {
        return new ColumnMask(0, name, catalog, schema, table, column, "NULL", List.of(), List.of(), enabled);
    }
}
