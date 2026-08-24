package org.pangrova.coordinator.index.lifecycle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IndexAutoRebuildTrigger} — verifies table name parsing
 * and guard clause logic.
 */
class IndexAutoRebuildTriggerTest {

    @Test
    void parseTableNameThreeParts() {
        String[] parts = IndexAutoRebuildTrigger.parseTableName("cat.sch.tbl");
        assertThat(parts).containsExactly("cat", "sch", "tbl");
    }

    @Test
    void parseTableNameTwoParts() {
        String[] parts = IndexAutoRebuildTrigger.parseTableName("sch.tbl");
        assertThat(parts).containsExactly("default", "sch", "tbl");
    }

    @Test
    void parseTableNameOnePart() {
        String[] parts = IndexAutoRebuildTrigger.parseTableName("tbl");
        assertThat(parts).containsExactly("default", "default", "tbl");
    }

    @Test
    void parseTableNameWithDots() {
        String[] parts = IndexAutoRebuildTrigger.parseTableName("my_catalog.my_schema.my_table");
        assertThat(parts).containsExactly("my_catalog", "my_schema", "my_table");
    }

    @Test
    void parseTableNameExtraDotsGoIntoTable() {
        // "a.b.c.d" → split(3) → ["a", "b", "c.d"]
        String[] parts = IndexAutoRebuildTrigger.parseTableName("a.b.c.d");
        assertThat(parts).containsExactly("a", "b", "c.d");
    }

    @Test
    void constructorRejectsNullMetadataStore() {
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> new IndexAutoRebuildTrigger(null, null));
    }
}
