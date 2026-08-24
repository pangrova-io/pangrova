package org.pangrova.protocol.rest;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.pangrova.protocol.rest.config.WriteConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the dead {@code transactionId} idempotency-key field has been
 * removed from both the REST DTO ({@link WriteConfig}) and the Protobuf
 * {@code WriteConfig} message, per architecture-remediation F-NATIVE-WRITE-04.
 *
 * <p>The field was parsed into native {@code WriteConfigInfo} but dropped before
 * the C++ {@code WriteDescriptor} with no journal or idempotent commit machinery
 * on either side. It was dead surface and has been removed. When doc 31 section 9
 * (Write Transaction Journal + idempotent {@code finishDistributedWrite}) is
 * implemented, these tests should be updated to assert the field is present
 * with full end-to-end consumption.</p>
 */
class WriteConfigTransactionIdCleanupTest {

    @Test
    @DisplayName("WriteConfig REST record does not contain transactionId component")
    void writeConfigRecordHasNoTransactionIdField() {
        Set<String> componentNames = Arrays.stream(WriteConfig.class.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());

        assertThat(componentNames)
                .as("WriteConfig record components should not include transactionId "
                        + "(removed per F-NATIVE-WRITE-04)")
                .doesNotContain("transactionId", "transaction_id");
    }

    @Test
    @DisplayName("WriteConfig REST record has exactly 5 fields after transactionId removal")
    void writeConfigRecordHasExpectedFieldCount() {
        RecordComponent[] components = WriteConfig.class.getRecordComponents();

        assertThat(components).hasSize(5);
        assertThat(Arrays.stream(components).map(RecordComponent::getName).toList())
                .containsExactly("targetTable", "writeMode", "partitionSpec",
                        "fileFormat", "targetFileSize");
    }

    @Test
    @DisplayName("Protobuf WriteConfig has no active transaction_id field")
    void protoWriteConfigHasNoActiveTransactionIdField() {
        Descriptor descriptor = org.pangrova.protocol.v1.WriteConfig.getDescriptor();
        List<String> activeFieldNames = descriptor.getFields().stream()
                .map(FieldDescriptor::getName)
                .toList();

        assertThat(activeFieldNames)
                .as("Proto WriteConfig active fields should not include transaction_id "
                        + "(reserved per F-NATIVE-WRITE-04)")
                .doesNotContain("transaction_id");
    }

    @Test
    @DisplayName("Protobuf WriteConfig reserves field number 4 for transaction_id")
    void protoWriteConfigReservesFieldNumber4() {
        Descriptor descriptor = org.pangrova.protocol.v1.WriteConfig.getDescriptor();

        // Field number 4 was transaction_id. It must be null (reserved, not active).
        FieldDescriptor field4 = descriptor.findFieldByNumber(4);
        assertThat(field4)
                .as("Proto WriteConfig field 4 (transaction_id) must be reserved, not active")
                .isNull();

        // Active fields should be: 1=target_table, 2=write_mode, 3=partition_spec,
        // 5=file_format, 6=target_file_size. Field 4 is skipped.
        Set<Integer> activeFieldNumbers = descriptor.getFields().stream()
                .map(FieldDescriptor::getNumber)
                .collect(Collectors.toSet());
        assertThat(activeFieldNumbers)
                .doesNotContain(4)
                .containsExactlyInAnyOrder(1, 2, 3, 5, 6);
    }

    @Test
    @DisplayName("WriteConfig can be constructed without transactionId argument")
    void writeConfigConstructorDoesNotAcceptTransactionId() {
        // The 5-arg constructor is the only canonical constructor for the record.
        WriteConfig config = new WriteConfig(
                "catalog.schema.table", "APPEND",
                List.of("region"), "parquet", 128_000_000);

        assertThat(config.targetTable()).isEqualTo("catalog.schema.table");
        assertThat(config.writeMode()).isEqualTo("APPEND");
        assertThat(config.partitionSpec()).containsExactly("region");
        assertThat(config.fileFormat()).isEqualTo("parquet");
        assertThat(config.targetFileSize()).isEqualTo(128_000_000);
    }
}
