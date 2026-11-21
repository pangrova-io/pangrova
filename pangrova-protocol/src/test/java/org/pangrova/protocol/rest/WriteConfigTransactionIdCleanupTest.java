package org.pangrova.protocol.rest;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.v2.WriteTaskOptions;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Ensures an unwired transaction identifier is absent from both public DTO and v2 wire schema. */
class WriteConfigTransactionIdCleanupTest {

    @Test
    void restWriteConfigHasOnlyImplementedFields() {
        assertThat(Arrays.stream(WriteConfig.class.getRecordComponents())
                .map(RecordComponent::getName).toList())
                .containsExactly("targetTable", "writeMode", "partitionSpec",
                        "fileFormat", "targetFileSize", "outputBindingBase64")
                .doesNotContain("transactionId", "transaction_id");
    }

    @Test
    void canonicalWriteOptionsContainOnlyImplementedFields() {
        assertThat(WriteTaskOptions.getDescriptor().getFields())
                .extracting(FieldDescriptor::getName)
                .containsExactly("target_table", "write_mode", "partition_spec",
                        "file_format", "target_file_size", "output_binding")
                .doesNotContain("transaction_id");
    }

    @Test
    void restWriteConfigCanBeConstructedWithoutDeadIdempotencySurface() {
        WriteConfig config = new WriteConfig(
                "catalog.schema.table", "APPEND",
                List.of("region"), "parquet", 128_000_000, "binding");

        assertThat(config.targetTable()).isEqualTo("catalog.schema.table");
        assertThat(config.partitionSpec()).containsExactly("region");
        assertThat(config.outputBindingBase64()).isEqualTo("binding");
    }
}
