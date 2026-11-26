package org.pangrova.indexing.api.descriptor;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata.Column;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.primitive.VarcharType;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexSchemaFingerprintTest {
    @Test
    void bindsIndexedNamesCaseInsensitivelyWithoutDependingOnAvailableColumnOrder() {
        var value = new Column("Value", PangrovaType.BIGINT, OptionalInt.of(7), null);
        var other = new Column("other", PangrovaType.VARCHAR, OptionalInt.of(8), null);

        String fingerprint = IndexSchemaFingerprint.of(List.of("value"), List.of(other, value));

        assertThat(fingerprint).hasSize(64);
        assertThat(IndexSchemaFingerprint.of(List.of("VALUE"), List.of(value, other)))
                .isEqualTo(fingerprint);
    }

    @Test
    void indexedColumnOrderIsPartOfTheBinding() {
        var first = new Column("first", PangrovaType.BIGINT, OptionalInt.of(1), null);
        var second = new Column("second", PangrovaType.VARCHAR, OptionalInt.of(2), null);

        assertThat(IndexSchemaFingerprint.of(List.of("first", "second"), List.of(first, second)))
                .isNotEqualTo(IndexSchemaFingerprint.of(List.of("second", "first"), List.of(first, second)));
    }

    @Test
    void parameterizedTypeAndFieldIdArePartOfTheBinding() {
        var shortText = new Column("value", PangrovaType.VARCHAR, OptionalInt.of(1), new VarcharType(10));
        var longText = new Column("value", PangrovaType.VARCHAR, OptionalInt.of(1), new VarcharType(20));
        var reassignedId = new Column("value", PangrovaType.VARCHAR, OptionalInt.of(2), new VarcharType(10));

        assertThat(IndexSchemaFingerprint.of(List.of("value"), List.of(shortText)))
                .isNotEqualTo(IndexSchemaFingerprint.of(List.of("value"), List.of(longText)))
                .isNotEqualTo(IndexSchemaFingerprint.of(List.of("value"), List.of(reassignedId)));
    }

    @Test
    void missingIndexedColumnIsRejected() {
        var value = new Column("value", PangrovaType.BIGINT, OptionalInt.of(1), null);

        assertThatThrownBy(() -> IndexSchemaFingerprint.of(List.of("missing"), List.of(value)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }
}
