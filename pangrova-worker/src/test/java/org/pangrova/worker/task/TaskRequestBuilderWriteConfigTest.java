package org.pangrova.worker.task;

import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.TaskRequest;
import org.pangrova.worker.task.convert.TaskRequestBuilder;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies WriteConfig is correctly serialized into TaskRequest protobuf bytes
 * and can be deserialized on the native side.
 */
class TaskRequestBuilderWriteConfigTest {

    @Test
    void writeConfigFieldsPreservedInProtobuf() throws InvalidProtocolBufferException {
        WriteConfig wc = new WriteConfig(
                "catalog.schema.orders", "APPEND",
                List.of("region", "date"), "parquet", 128_000_000);
        SubmitTaskRequest request = buildRequest(wc);

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(bytes);

        assertThat(parsed.getWriteConfig().getTargetTable()).isEqualTo("catalog.schema.orders");
        assertThat(parsed.getWriteConfig().getWriteMode()).isEqualTo("APPEND");
        assertThat(parsed.getWriteConfig().getPartitionSpecList()).containsExactly("region", "date");
        assertThat(parsed.getWriteConfig().getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
        assertThat(parsed.getWriteConfig().getTargetFileSize()).isEqualTo(128_000_000);
    }

    @Test
    void nullWriteConfigProducesEmptyProtoField() throws InvalidProtocolBufferException {
        SubmitTaskRequest request = buildRequest(null);

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(bytes);

        assertThat(parsed.hasWriteConfig()).isFalse();
    }

    @Test
    void overwriteModePreserved() throws InvalidProtocolBufferException {
        WriteConfig wc = new WriteConfig(
                "db.table", "OVERWRITE", List.of(), "orc", 64_000_000);
        SubmitTaskRequest request = buildRequest(wc);

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(bytes);

        assertThat(parsed.getWriteConfig().getWriteMode()).isEqualTo("OVERWRITE");
        assertThat(parsed.getWriteConfig().getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_ORC);
    }

    private static SubmitTaskRequest buildRequest(WriteConfig writeConfig) {
        String plan = Base64.getEncoder().encodeToString(new byte[]{0x01});
        return SubmitTaskRequest.builder()
                .taskId("task-w1")
                .queryId("query-w1")
                .stageId(0)
                .partitionId(0)
                .taskType("WRITE")
                .planFragment(plan)
                .splits(null)
                .exchangeSources(null)
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(writeConfig)
                .build();
    }
}
