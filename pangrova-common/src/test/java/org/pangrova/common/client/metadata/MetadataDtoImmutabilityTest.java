package org.pangrova.common.client.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies catalog metadata DTOs are records with defensively copied collections.
 */
class MetadataDtoImmutabilityTest {

    @Test
    void tableIdentifierNamespaceIsDefensivelyCopied() {
        List<String> namespace = new ArrayList<>(List.of("db"));
        MetadataTableIdentifier identifier = new MetadataTableIdentifier(namespace, "t");

        namespace.add("injected");

        assertThat(identifier.namespace()).containsExactly("db");
        assertThatThrownBy(() -> identifier.namespace().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void tableInfoCopiesNamespaceAndColumns() {
        List<String> namespace = new ArrayList<>(List.of("db"));
        List<MetadataColumnInfo> columns = new ArrayList<>(
                List.of(new MetadataColumnInfo(1, "id", "BIGINT", true)));
        MetadataTableInfo info = new MetadataTableInfo(namespace, "t", columns);

        namespace.add("injected");
        columns.add(new MetadataColumnInfo(2, "name", "VARCHAR", false));

        assertThat(info.namespace()).containsExactly("db");
        assertThat(info.columns()).hasSize(1);
        assertThatThrownBy(() -> info.columns().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void metadataDtosAreRecords() {
        assertThat(MetadataTableIdentifier.class.isRecord()).isTrue();
        assertThat(MetadataTableInfo.class.isRecord()).isTrue();
        assertThat(MetadataColumnInfo.class.isRecord()).isTrue();
    }
}
