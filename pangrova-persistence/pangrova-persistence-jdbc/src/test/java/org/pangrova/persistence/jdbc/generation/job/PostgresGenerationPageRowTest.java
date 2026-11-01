package org.pangrova.persistence.jdbc.generation.job;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.generation.GenerationPageKind;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests nullable persisted-buffer ownership only, not Native report validity or durable completion. */
class PostgresGenerationPageRowTest {
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

    private static PostgresGenerationJobStoreMappings.PageRow row(byte[] bytes) {
        return new PostgresGenerationJobStoreMappings.PageRow("tenant", "job", "page", 0, 0, 0,
                GenerationPageKind.SOURCE_MAP, "PENDING", 0, 0, null, 0, null,
                3, bytes, null, null);
    }
}
