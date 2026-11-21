package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.MetadataTableKind;

import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataTableKindTest {
    @ParameterizedTest
    @EnumSource(MetadataTableKind.class)
    void recognizedKindsPreserveTheExactBaseIdentity(MetadataTableKind kind) {
        String relation = "Lake.Db.Orders$" + kind.suffix().toUpperCase(Locale.ROOT);
        assertThat(MetadataTableKind.fromTableName(relation)).contains(kind);
        assertThat(MetadataTableKind.baseTableName(relation)).isEqualTo("Lake.Db.Orders");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"Orders", "$refs", "Orders$", "Orders$unknown"})
    void unsupportedOrIncompleteNamesDoNotAcquireMetadataSemantics(String table) {
        assertThat(MetadataTableKind.fromTableName(table)).isEmpty();
        assertThat(MetadataTableKind.baseTableName(table)).isEqualTo(table);
    }

    @Test
    void baseContainingDollarIsNotTruncatedAtTheWrongSeparator() {
        assertThat(MetadataTableKind.baseTableName("Orders$archive$refs")).isEqualTo("Orders$archive");
    }
}
