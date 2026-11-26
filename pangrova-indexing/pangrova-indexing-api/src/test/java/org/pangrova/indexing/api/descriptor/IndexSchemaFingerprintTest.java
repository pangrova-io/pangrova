package org.pangrova.indexing.api.descriptor;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata.Column;
import org.pangrova.types.RowField;
import org.pangrova.types.RowType;
import org.pangrova.types.Type;
import org.pangrova.types.TypeSignature;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.primitive.VarcharType;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexSchemaFingerprintTest {
    @Test
    void canonicalGoldenVectorDoesNotDependOnAvailableColumnOrder() {
        var value = new Column("Value", PangrovaType.BIGINT, OptionalInt.of(7), null);
        var other = new Column("other", PangrovaType.VARCHAR, OptionalInt.of(8), null);
        String fingerprint = IndexSchemaFingerprint.of(List.of("Value"), List.of(other, value));

        assertThat(fingerprint).isEqualTo("v2:7db95360e0ea2399e6d2f751b8d4b89ebe83296fad36c06179c4618147bf2db9");
        assertThat(IndexSchemaFingerprint.isCurrent(fingerprint)).isTrue();
        assertThat(IndexSchemaFingerprint.of(List.of("Value"), List.of(value, other))).isEqualTo(fingerprint);
        assertThat(IndexSchemaFingerprint.isCurrent("a".repeat(64))).isFalse();
        assertThat(IndexSchemaFingerprint.isCurrent("v3:" + "a".repeat(64))).isFalse();
    }

    @Test
    void userResolutionPrefersExactNameAndRejectsAmbiguousFallback() {
        var upper = new Column("Value", PangrovaType.BIGINT, OptionalInt.of(7), null);
        var lower = new Column("value", PangrovaType.BIGINT, OptionalInt.of(8), null);
        assertThat(IndexSchemaFingerprint.resolveColumn("Value", List.of(lower, upper))).isEqualTo(upper);
        assertThat(IndexSchemaFingerprint.resolveColumn("VALUE", List.of(upper))).isEqualTo(upper);
        assertThatThrownBy(() -> IndexSchemaFingerprint.resolveColumn("VALUE", List.of(upper, lower)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ambiguous");
        assertThatThrownBy(() -> IndexSchemaFingerprint.of(List.of("Value"), List.of(lower)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("absent");
        assertThatThrownBy(() -> IndexSchemaFingerprint.of(List.of("Value"), List.of(upper, upper)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Ambiguous");
        assertThatThrownBy(() -> IndexSchemaFingerprint.of(List.of("Value", "Value"), List.of(upper)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
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
    void structuredTypeSignatureAndOptionalFieldIdAreFramedIndependentlyOfDisplayText() {
        var row = new RowType(List.of(new RowField("field, 名", new VarcharType(10))));
        Type equivalent = new Type() {
            @Override public String getDisplayName() { return "different presentation"; }
            @Override public TypeSignature getTypeSignature() { return row.getTypeSignature(); }
        };
        var original = new Column("value", PangrovaType.STRUCT, OptionalInt.empty(), row);
        var same = new Column("value", PangrovaType.STRUCT, OptionalInt.empty(), equivalent);
        var withId = new Column("value", PangrovaType.STRUCT, OptionalInt.of(0), row);
        var another = new Column("value", PangrovaType.STRUCT, OptionalInt.empty(),
                new RowType(List.of(new RowField("field, 名", new VarcharType(11)))));
        String fingerprint = IndexSchemaFingerprint.of(List.of("value"), List.of(original));

        assertThat(IndexSchemaFingerprint.of(List.of("value"), List.of(same))).isEqualTo(fingerprint);
        assertThat(IndexSchemaFingerprint.of(List.of("value"), List.of(withId))).isNotEqualTo(fingerprint);
        assertThat(IndexSchemaFingerprint.of(List.of("value"), List.of(another))).isNotEqualTo(fingerprint);
    }

    @Test
    void missingIndexedColumnIsRejected() {
        var value = new Column("value", PangrovaType.BIGINT, OptionalInt.of(1), null);

        assertThatThrownBy(() -> IndexSchemaFingerprint.of(List.of("missing"), List.of(value)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }
}
