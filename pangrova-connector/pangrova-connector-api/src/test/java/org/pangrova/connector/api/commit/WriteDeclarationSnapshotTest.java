package org.pangrova.connector.api.commit;

import org.pangrova.types.enums.VectorElementType;

import org.junit.jupiter.api.Test;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteDeclarationSnapshotTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("catalog", "schema", "documents");

    @Property(tries = 100, seed = "3419027")
    void exactFieldAndVectorDeclarationHasStableContentIdentity(
            @ForAll @IntRange(min = 1, max = 10000) int fieldId,
            @ForAll @IntRange(min = 1, max = 4096) int dimensions) {
        var snapshot = capture(List.of(column("id", fieldId), vector(dimensions)));
        assertThat(snapshot).isEqualTo(capture(List.of(column("id", fieldId), vector(dimensions))));
        assertThat(snapshot.schemaSha256())
                .isNotEqualTo(capture(List.of(column("id", fieldId + 1), vector(dimensions))).schemaSha256())
                .isNotEqualTo(capture(List.of(column("id", fieldId), vector(dimensions + 1))).schemaSha256());
    }

    @Test
    void capturesOrderedNamesTypesFieldIdentityAndVectorDimensions() {
        var original = capture(List.of(column("id", 1), vector(3)));
        assertThat(original).isEqualTo(capture(List.of(column("id", 1), vector(3))));
        assertThat(original.schemaSha256()).isNotEqualTo(capture(List.of(vector(3), column("id", 1))).schemaSha256())
                .isNotEqualTo(capture(List.of(column("renamed", 1), vector(3))).schemaSha256())
                .isNotEqualTo(capture(List.of(column("id", 5), vector(3))).schemaSha256())
                .isNotEqualTo(capture(List.of(column("id", 1), vector(4))).schemaSha256());
        assertThat(original.generatedColumnsSha256()).isEqualTo(WriteDeclarationSnapshot.generatedColumnsDigest(new byte[]{1}));
    }

    @Test
    void rejectsUnknownOrMisorderedSchemaDespiteEqualColumnCount() {
        for (var typed : List.of(List.<ConnectorTableMetadata.Column>of(),
                List.of(column("other", 1)), List.of(column("id", 1), vector(3)))) {
            var metadata = new ConnectorTableMetadata(new SchemaTableName("schema", "documents"),
                    List.of("id"), typed, Map.of());
            assertThatThrownBy(() -> WriteDeclarationSnapshot.capture(TABLE, metadata, new byte[]{1}, false))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("complete actual connector schema");
        }
    }

    @Test
    void rejectsOtherTableOrMalformedDigest() {
        var metadata = new ConnectorTableMetadata(new SchemaTableName("schema", "other"), List.of("id"),
                List.of(column("id", 1)), Map.of());
        assertThatThrownBy(() -> WriteDeclarationSnapshot.capture(TABLE, metadata, new byte[]{1}, false))
                .isInstanceOf(IllegalArgumentException.class);
        for (var digest : List.of("", "A".repeat(64), "a".repeat(63), "a".repeat(65))) {
            assertThatThrownBy(() -> new WriteDeclarationSnapshot(TABLE, digest, "b".repeat(64), false))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new WriteDeclarationSnapshot(null, "a".repeat(64), "b".repeat(64), false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> WriteDeclarationSnapshot.generatedColumnsDigest(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static WriteDeclarationSnapshot capture(List<ConnectorTableMetadata.Column> columns) {
        return WriteDeclarationSnapshot.capture(TABLE, new ConnectorTableMetadata(
                new SchemaTableName("schema", "documents"),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of()), new byte[]{1}, false);
    }

    private static ConnectorTableMetadata.Column column(String name, int fieldId) {
        return new ConnectorTableMetadata.Column(name, PangrovaType.BIGINT, OptionalInt.of(fieldId));
    }

    private static ConnectorTableMetadata.Column vector(int dimensions) {
        return new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR, OptionalInt.of(2),
                new VectorType(dimensions, VectorElementType.FLOAT));
    }
}
