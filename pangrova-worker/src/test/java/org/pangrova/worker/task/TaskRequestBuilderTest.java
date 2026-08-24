package org.pangrova.worker.task;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.v1.TaskRequest;
import org.pangrova.protocol.v1.TaskType;
import org.pangrova.worker.task.convert.TaskRequestBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for TaskRequestBuilder.
 * Validates: Requirements 2.2, 2.6
 */
class TaskRequestBuilderTest {

    @Test
    void build_mapsAllFieldsToProtobuf() throws InvalidProtocolBufferException {
        byte[] planBytes = {0x01, 0x02, 0x03};
        SplitInfo split = SplitInfo.newBuilder()
                .setSplitId("s1")
                .setConnectorId("iceberg")
                .setFilePath("/data/part-0.parquet")
                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                .setStartOffset(100)
                .setLength(5000)
                .build();
        SubmitTaskRequest request = createFullRequest(planBytes, List.of(split));

        byte[] result = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(result);

        assertThat(parsed.getTaskId()).isEqualTo("task-1");
        assertThat(parsed.getQueryId()).isEqualTo("query-1");
        assertThat(parsed.getStageId()).isEqualTo(2);
        assertThat(parsed.getPartitionId()).isEqualTo(3);
        assertThat(parsed.getTaskType()).isEqualTo(TaskType.TASK_TYPE_READ);
        assertThat(parsed.getPlanFragment().toByteArray()).isEqualTo(planBytes);
        assertThat(parsed.getSplitsList()).hasSize(1);
        assertThat(parsed.getSplits(0).getSplitId()).isEqualTo("s1");
        assertThat(parsed.getSplits(0).getConnectorId()).isEqualTo("iceberg");
        assertThat(parsed.getSplits(0).getFilePath()).isEqualTo("/data/part-0.parquet");
        assertThat(parsed.getSplits(0).getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
        assertThat(parsed.getSplits(0).getStartOffset()).isEqualTo(100);
        assertThat(parsed.getSplits(0).getLength()).isEqualTo(5000);
        assertThat(parsed.getSessionPropertiesMap()).containsEntry("key", "value");
        assertThat(parsed.getResourceLimit().getMaxMemoryBytes()).isEqualTo(1024L);
    }

    @Test
    void build_throwsOnNullRequest() {
        assertThatThrownBy(() -> TaskRequestBuilder.build(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("request must not be null");
    }

    @Test
    void build_throwsOnNullPlanFragment() {
        SubmitTaskRequest request = createRequest(null, null);
        assertThatThrownBy(() -> TaskRequestBuilder.build(request))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("planFragment must not be null");
    }

    @Test
    void build_excludesJavaOnlyFields() throws InvalidProtocolBufferException {
        SubmitTaskRequest request = createRequestWithJavaOnlyFields();

        byte[] result = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(result);

        // exchangeSources and outputConfig are Java-layer managed, not serialized to protobuf.
        // writeConfig IS now forwarded to native via protobuf.
        assertThat(parsed.getTaskId()).isEqualTo("task-1");
        assertThat(parsed.getQueryId()).isEqualTo("query-1");
        assertThat(parsed.getWriteConfig().getTargetTable()).isEqualTo("table");
        assertThat(parsed.getWriteConfig().getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
    }

    @Test
    void build_handlesNullSplitsAndSessionProperties() throws InvalidProtocolBufferException {
        SubmitTaskRequest request = createRequest(
                Base64.getEncoder().encodeToString(new byte[]{0x01}), null);

        byte[] result = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(result);

        assertThat(parsed.getSplitsList()).isEmpty();
        assertThat(parsed.getSessionPropertiesMap()).isEmpty();
    }

    @Test
    void decodeSplitInfos_decodesAllSplits() {
        SplitInfo split1 = SplitInfo.newBuilder().setSplitId("s1").build();
        SplitInfo split2 = SplitInfo.newBuilder().setSplitId("s2").build();
        List<String> encoded = List.of(
                Base64.getEncoder().encodeToString(split1.toByteArray()),
                Base64.getEncoder().encodeToString(split2.toByteArray()));

        List<SplitInfo> result = TaskRequestBuilder.decodeSplitInfos(encoded);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getSplitId()).isEqualTo("s1");
        assertThat(result.get(1).getSplitId()).isEqualTo("s2");
    }

    @Test
    void decodeSplitInfos_returnsEmptyForNull() {
        assertThat(TaskRequestBuilder.decodeSplitInfos(null)).isEmpty();
    }

    @Test
    void decodeSplitInfos_returnsEmptyForEmptyList() {
        assertThat(TaskRequestBuilder.decodeSplitInfos(List.of())).isEmpty();
    }

    private static SubmitTaskRequest createFullRequest(byte[] planBytes, List<SplitInfo> splits) {
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        List<String> encodedSplits = splits == null ? null : splits.stream()
                .map(s -> Base64.getEncoder().encodeToString(s.toByteArray()))
                .toList();
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(2)
                .partitionId(3)
                .taskType("COMPUTE")
                .planFragment(encodedPlan)
                .splits(encodedSplits)
                .exchangeSources(null)
                .resourceLimit(new ResourceLimitPayload(1024L, 4, 2048L, "/tmp/spill"))
                .sessionProperties(Map.of("key", "value"))
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private static SubmitTaskRequest createRequest(String planFragment, List<String> splits) {
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(planFragment)
                .splits(splits)
                .exchangeSources(null)
                .resourceLimit(null)
                .sessionProperties(null)
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private static SubmitTaskRequest createRequestWithJavaOnlyFields() {
        String encodedPlan = Base64.getEncoder().encodeToString(new byte[]{0x01});
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(encodedPlan)
                .splits(null)
                .exchangeSources(List.of(new org.pangrova.protocol.rest.task.ExchangeSourceDescriptor(
                        "src-1", "host1", 9090, 0, "HASH", 1L)))
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(new org.pangrova.protocol.rest.config.OutputConfig("ticket", 1024, 10, 5000))
                .writeConfig(new org.pangrova.protocol.rest.config.WriteConfig("table", "APPEND", List.of(), "parquet", 128_000_000L))
                .build();
    }
}
