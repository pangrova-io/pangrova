package org.pangrova.indexing.store.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.ArtifactBuildPageKind;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests nullable persisted-buffer ownership only, not Native report validity or durable completion. */
class JdbcArtifactBuildPageRowTest {
    @Test
    void reportBufferCannotBeChangedByItsProducerOrReader() {
        byte[] bytes = {1, 2, 3};
        var row = row(bytes);
        bytes[0] = 4;
        byte[] exported = row.reportBytes();
        assertThat(exported).containsExactly((byte) 1, (byte) 2, (byte) 3);
        exported[1] = 5;
        assertThat(row.reportBytes()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void anUnreportedPageKeepsAnAbsentReportRatherThanFabricatingBytes() {
        assertThat(row(null).reportBytes()).isNull();
    }

    private static JdbcArtifactBuildStoreMappings.PageRow row(byte[] bytes) {
        return new JdbcArtifactBuildStoreMappings.PageRow("tenant", "job", "page", 0, 0, 0,
                ArtifactBuildPageKind.SOURCE_MAP, "PENDING", 0, 0, null, 0, null,
                3, bytes, null, null);
    }
}
