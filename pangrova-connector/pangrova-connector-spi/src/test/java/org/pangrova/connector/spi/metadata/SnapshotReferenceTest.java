package org.pangrova.connector.spi.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotReferenceTest {

    @Test
    void unsupportedConnectorFailsClosed() {
        ConnectorSnapshotMetadata metadata = new ConnectorSnapshotMetadata() {};
        assertThatThrownBy(() -> metadata.listSnapshotReferences(null, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("listSnapshotReferences");
    }

    @Test
    void invalidReferenceCannotEnterMaintenancePath() {
        assertThatThrownBy(() -> new SnapshotReference("", SnapshotReference.Type.BRANCH, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SnapshotReference("feature", null, 1))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SnapshotReference("feature", SnapshotReference.Type.BRANCH, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
