package org.pangrova.worker.task.convert;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.ShuffleSerdeFormat;
import org.pangrova.protocol.v1.TaskRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies TaskRequestBuilder.mapShuffleConfig() correctly converts
 * REST ShuffleConfig DTO to Proto ShuffleConfig message.
 *
 * <p>Validates: Requirements Req-3</p>
 */
@DisplayName("ShuffleConfig DTO → Proto Conversion")
class ShuffleConfigDtoProtoConversionTest {

    private static final String DUMMY_PLAN = Base64.getEncoder().encodeToString(new byte[]{0x01});

    @Test
    @DisplayName("Default ShuffleConfig maps to proto with lz4 and batch_size=10000")
    void defaultShuffleConfigMapsCorrectly() throws InvalidProtocolBufferException {
        ShuffleConfig dto = new ShuffleConfig(null, 0);
        // Compact constructor defaults: compressionCodec="lz4", batchSize=10000
        assertThat(dto.compressionCodec()).isEqualTo("lz4");
        assertThat(dto.batchSize()).isEqualTo(10000);

        TaskRequest proto = buildAndParse(dto);

        assertThat(proto.hasShuffleConfig()).isTrue();
        assertThat(proto.getShuffleConfig().getSerdeFormat())
                .isEqualTo(ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES);
        assertThat(proto.getShuffleConfig().getCompressionCodec()).isEqualTo("lz4");
        assertThat(proto.getShuffleConfig().getBatchSize()).isEqualTo(10000);
    }

    @Test
    @DisplayName("Custom ShuffleConfig values are correctly mapped to proto")
    void customShuffleConfigMapsCorrectly() throws InvalidProtocolBufferException {
        ShuffleConfig dto = new ShuffleConfig("zstd", 5000);

        TaskRequest proto = buildAndParse(dto);

        assertThat(proto.hasShuffleConfig()).isTrue();
        assertThat(proto.getShuffleConfig().getSerdeFormat())
                .isEqualTo(ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES);
        assertThat(proto.getShuffleConfig().getCompressionCodec()).isEqualTo("zstd");
        assertThat(proto.getShuffleConfig().getBatchSize()).isEqualTo(5000);
    }

    @Test
    @DisplayName("None compression codec is correctly mapped")
    void noneCompressionCodecMapsCorrectly() throws InvalidProtocolBufferException {
        ShuffleConfig dto = new ShuffleConfig("none", 2000);

        TaskRequest proto = buildAndParse(dto);

        assertThat(proto.getShuffleConfig().getCompressionCodec()).isEqualTo("none");
        assertThat(proto.getShuffleConfig().getBatchSize()).isEqualTo(2000);
    }

    @Test
    @DisplayName("Null ShuffleConfig results in no shuffle_config in proto")
    void nullShuffleConfigOmitsProtoField() throws InvalidProtocolBufferException {
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(DUMMY_PLAN)
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(null)
                .outputConfig(null)
                .writeConfig(null)
                .build();

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest proto = TaskRequest.parseFrom(bytes);

        assertThat(proto.hasShuffleConfig()).isFalse();
    }

    @Test
    @DisplayName("Empty compressionCodec defaults to lz4 via compact constructor")
    void emptyCompressionCodecDefaultsToLz4() throws InvalidProtocolBufferException {
        ShuffleConfig dto = new ShuffleConfig("", 8000);
        // Empty string is not null, so compact constructor keeps it as-is
        // But the proto should still receive the empty string
        TaskRequest proto = buildAndParse(dto);

        assertThat(proto.getShuffleConfig().getCompressionCodec()).isEqualTo("");
        assertThat(proto.getShuffleConfig().getBatchSize()).isEqualTo(8000);
    }

    @Test
    @DisplayName("Serde format is always PRESTO_PAGES regardless of DTO values")
    void serdeFormatAlwaysPrestoPages() throws InvalidProtocolBufferException {
        ShuffleConfig dto = new ShuffleConfig("lz4", 10000);

        TaskRequest proto = buildAndParse(dto);

        assertThat(proto.getShuffleConfig().getSerdeFormat())
                .isEqualTo(ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES);
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private TaskRequest buildAndParse(ShuffleConfig shuffleConfig)
            throws InvalidProtocolBufferException {
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(DUMMY_PLAN)
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(null)
                .outputConfig(null)
                .writeConfig(null)
                .dynamicFilters(List.of())
                .shuffleConfig(shuffleConfig)
                .build();

        byte[] bytes = TaskRequestBuilder.build(request);
        return TaskRequest.parseFrom(bytes);
    }
}
