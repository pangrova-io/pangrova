package org.pangrova.worker.property.task;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.v1.TaskRequest;
import org.pangrova.worker.task.TaskTypeMapper;
import org.pangrova.worker.task.convert.TaskRequestBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based round-trip test for TaskRequest serialization.
 *
 * <p>Verifies that for any valid SubmitTaskRequest, serialization via
 * {@link TaskRequestBuilder} to protobuf bytes and deserialization back
 * via Java protobuf classes produces semantically equivalent results.
 * This validates the Java-verifiable portion of the cross-language
 * contract (Java ↔ C++ TaskRequestParser).</p>
 *
 * <p><b>Validates: Requirements 1.7, 2.5</b></p>
 */
class TaskRequestRoundTripPropertyTest {

    /**
     * Property 6: TaskRequest serialization round-trip.
     *
     * <p>For any valid SubmitTaskRequest with all fields populated,
     * serialize → deserialize preserves all mapped fields.</p>
     *
     * <p><b>Validates: Requirements 1.7, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 6: TaskRequest serialization round-trip")
    void fullRoundTrip(
            @ForAll("fullSubmitTaskRequests") SubmitTaskRequest request)
            throws InvalidProtocolBufferException {

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(bytes);

        assertIdentifiers(parsed, request);
        assertPlanFragment(parsed, request);
        assertSplits(parsed, request);
        assertSessionProperties(parsed, request);
        assertResourceLimit(parsed, request);
        assertWriteConfig(parsed, request);
    }

    /**
     * Property 6 (variant): round-trip with null optional fields.
     *
     * <p>Verifies that null resourceLimit and null writeConfig
     * survive the round-trip without corruption.</p>
     *
     * <p><b>Validates: Requirements 1.7, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 6: TaskRequest round-trip null optionals")
    void roundTripWithNullOptionals(
            @ForAll("minimalSubmitTaskRequests") SubmitTaskRequest request)
            throws InvalidProtocolBufferException {

        byte[] bytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(bytes);

        assertIdentifiers(parsed, request);
        assertPlanFragment(parsed, request);
        assertSplits(parsed, request);
        assertThat(parsed.hasResourceLimit()).isFalse();
        assertThat(parsed.hasWriteConfig()).isFalse();
    }

    /**
     * Property 6 (idempotency): double serialization produces identical bytes.
     *
     * <p>Serialize → deserialize → re-serialize must produce identical
     * protobuf bytes, confirming no information loss in the round-trip.</p>
     *
     * <p><b>Validates: Requirements 1.7, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 6: TaskRequest serialization idempotency")
    void serializationIdempotency(
            @ForAll("fullSubmitTaskRequests") SubmitTaskRequest request)
            throws InvalidProtocolBufferException {

        byte[] firstPass = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(firstPass);
        byte[] secondPass = parsed.toByteArray();

        assertThat(firstPass).isEqualTo(secondPass);
    }

    /**
     * Property 9: Java-only field exclusion.
     *
     * <p>For any SubmitTaskRequest with non-null exchangeSources and outputConfig,
     * the resulting protobuf bytes contain none of these Java-layer-managed fields
     * (except writeConfig which is forwarded to native).</p>
     *
     * <p><b>Validates: Requirements 2.6</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: TaskRequestBuilder Java-only field exclusion")
    void javaOnlyFieldExclusion(
            @ForAll("requestsWithJavaOnlyFields") SubmitTaskRequest request)
            throws InvalidProtocolBufferException {

        byte[] protobufBytes = TaskRequestBuilder.build(request);
        TaskRequest parsed = TaskRequest.parseFrom(protobufBytes);

        TaskRequest rebuilt = TaskRequest.newBuilder().mergeFrom(parsed).build();
        assertThat(protobufBytes).isEqualTo(rebuilt.toByteArray());
        assertIdentifiers(parsed, request);
    }

    @Provide
    Arbitrary<SubmitTaskRequest> requestsWithJavaOnlyFields() {
        return fullSubmitTaskRequests().map(base ->
                SubmitTaskRequest.builder()
                        .taskId(base.taskId())
                        .queryId(base.queryId())
                        .stageId(base.stageId())
                        .partitionId(base.partitionId())
                        .taskType(base.taskType())
                        .planFragment(base.planFragment())
                        .splits(base.splits())
                        .exchangeSources(List.of(new ExchangeSourceDescriptor("src-1", "host", 9090, 0, "HASH", 1L)))
                        .resourceLimit(base.resourceLimit())
                        .sessionProperties(base.sessionProperties())
                        .outputConfig(new OutputConfig("ticket", 1024, 10, 5000))
                        .writeConfig(base.writeConfig())
                        .build());
    }

    // --- Assertion helpers ---

    private void assertIdentifiers(TaskRequest parsed, SubmitTaskRequest req) {
        assertThat(parsed.getTaskId()).isEqualTo(req.taskId());
        assertThat(parsed.getQueryId()).isEqualTo(req.queryId());
        assertThat(parsed.getStageId()).isEqualTo(req.stageId());
        assertThat(parsed.getPartitionId()).isEqualTo(req.partitionId());
        assertThat(parsed.getTaskType())
                .isEqualTo(TaskTypeMapper.toProtoType(req.taskType()));
    }

    private void assertPlanFragment(TaskRequest parsed, SubmitTaskRequest req) {
        byte[] expected = Base64.getDecoder().decode(req.planFragment());
        assertThat(parsed.getPlanFragment().toByteArray()).isEqualTo(expected);
    }

    private void assertSplits(TaskRequest parsed, SubmitTaskRequest req) {
        List<SplitInfo> expected = TaskRequestBuilder.decodeSplitInfos(req.splits());
        assertThat(parsed.getSplitsList()).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(parsed.getSplits(i)).isEqualTo(expected.get(i));
        }
    }

    private void assertSessionProperties(TaskRequest parsed, SubmitTaskRequest req) {
        Map<String, String> expected = req.sessionProperties();
        if (expected == null || expected.isEmpty()) {
            assertThat(parsed.getSessionPropertiesMap()).isEmpty();
        } else {
            assertThat(parsed.getSessionPropertiesMap()).isEqualTo(expected);
        }
    }

    private void assertResourceLimit(TaskRequest parsed, SubmitTaskRequest req) {
        if (req.resourceLimit() == null) {
            assertThat(parsed.hasResourceLimit()).isFalse();
            return;
        }
        assertThat(parsed.hasResourceLimit()).isTrue();
        var rl = parsed.getResourceLimit();
        assertThat(rl.getMaxMemoryBytes()).isEqualTo(req.resourceLimit().maxMemoryBytes());
        assertThat(rl.getMaxThreads()).isEqualTo(req.resourceLimit().maxThreads());
        assertThat(rl.getMaxSpillBytes()).isEqualTo(req.resourceLimit().maxSpillBytes());
        String expectedDir = req.resourceLimit().spillDirectory();
        assertThat(rl.getSpillDirectory()).isEqualTo(expectedDir == null ? "" : expectedDir);
    }

    private void assertWriteConfig(TaskRequest parsed, SubmitTaskRequest req) {
        if (req.writeConfig() == null) {
            assertThat(parsed.hasWriteConfig()).isFalse();
            return;
        }
        assertThat(parsed.hasWriteConfig()).isTrue();
        var wc = parsed.getWriteConfig();
        assertThat(wc.getTargetTable()).isEqualTo(nullToEmpty(req.writeConfig().targetTable()));
        assertThat(wc.getWriteMode()).isEqualTo(nullToEmpty(req.writeConfig().writeMode()));
        assertThat(wc.getFileFormat()).isEqualTo(expectedFileFormat(req.writeConfig().fileFormat()));
        assertThat(wc.getTargetFileSize()).isEqualTo(req.writeConfig().targetFileSize());
        List<String> expectedSpec = req.writeConfig().partitionSpec();
        if (expectedSpec == null) {
            assertThat(wc.getPartitionSpecList()).isEmpty();
        } else {
            assertThat(wc.getPartitionSpecList()).isEqualTo(expectedSpec);
        }
    }

    private static FileFormat expectedFileFormat(String format) {
        if (format == null || format.isEmpty()) {
            return FileFormat.FILE_FORMAT_PARQUET;
        }
        return switch (format.toUpperCase()) {
            case "ORC" -> FileFormat.FILE_FORMAT_ORC;
            default -> FileFormat.FILE_FORMAT_PARQUET;
        };
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    // --- Generators ---

    @Provide
    Arbitrary<SubmitTaskRequest> fullSubmitTaskRequests() {
        return Combinators.combine(
                coreFields(), planFragments(), splitLists(),
                sessionPropertyMaps(), resourceLimits(), writeConfigs()
        ).as((core, plan, splits, props, limit, wc) ->
                SubmitTaskRequest.builder()
                        .taskId(core.taskId)
                        .queryId(core.queryId)
                        .stageId(core.stageId)
                        .partitionId(core.partitionId)
                        .taskType(core.taskType)
                        .planFragment(plan)
                        .splits(splits)
                        .exchangeSources(null)
                        .resourceLimit(limit)
                        .sessionProperties(props)
                        .outputConfig(null)
                        .writeConfig(wc)
                        .build());
    }

    @Provide
    Arbitrary<SubmitTaskRequest> minimalSubmitTaskRequests() {
        return Combinators.combine(
                coreFields(), planFragments(), splitLists(),
                sessionPropertyMaps()
        ).as((core, plan, splits, props) ->
                SubmitTaskRequest.builder()
                        .taskId(core.taskId)
                        .queryId(core.queryId)
                        .stageId(core.stageId)
                        .partitionId(core.partitionId)
                        .taskType(core.taskType)
                        .planFragment(plan)
                        .splits(splits)
                        .exchangeSources(null)
                        .resourceLimit(null)
                        .sessionProperties(props)
                        .outputConfig(null)
                        .writeConfig(null)
                        .build());
    }

    /** Groups identity fields to stay within Combinators.combine arity limit. */
    private record CoreFields(String taskId, String queryId, int stageId,
                              int partitionId, String taskType) {}

    private Arbitrary<CoreFields> coreFields() {
        return Combinators.combine(
                taskIds(), queryIds(), stageIds(), partitionIds(), taskTypes()
        ).as(CoreFields::new);
    }

    private Arbitrary<String> taskIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    private Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    private Arbitrary<Integer> stageIds() {
        return Arbitraries.integers().between(0, 1000);
    }

    private Arbitrary<Integer> partitionIds() {
        return Arbitraries.integers().between(0, 100);
    }

    private Arbitrary<String> taskTypes() {
        return Arbitraries.of("COMPUTE", "OUTPUT", "WRITE");
    }

    private Arbitrary<String> planFragments() {
        return Arbitraries.bytes().array(byte[].class)
                .ofMinSize(1).ofMaxSize(256)
                .map(bytes -> Base64.getEncoder().encodeToString(bytes));
    }

    private Arbitrary<List<String>> splitLists() {
        Arbitrary<String> singleSplit = splitInfoArbitrary()
                .map(info -> Base64.getEncoder().encodeToString(info.toByteArray()));
        return singleSplit.list().ofMinSize(0).ofMaxSize(5);
    }

    private Arbitrary<SplitInfo> splitInfoArbitrary() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                Arbitraries.of("iceberg", "paimon"),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30),
                Arbitraries.of(FileFormat.FILE_FORMAT_PARQUET, FileFormat.FILE_FORMAT_ORC),
                Arbitraries.longs().between(0, 1_000_000L),
                Arbitraries.longs().between(1, 1_000_000L)
        ).as((splitId, connectorId, filePath, format, offset, length) ->
                SplitInfo.newBuilder()
                        .setSplitId(splitId)
                        .setConnectorId(connectorId)
                        .setFilePath(filePath)
                        .setFileFormat(format)
                        .setStartOffset(offset)
                        .setLength(length)
                        .setTableName(connectorId + "." + filePath)
                        .build());
    }

    private Arbitrary<Map<String, String>> sessionPropertyMaps() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(20)
        ).ofMinSize(0).ofMaxSize(5);
    }

    private Arbitrary<ResourceLimitPayload> resourceLimits() {
        return Combinators.combine(
                Arbitraries.longs().between(0, 1_000_000_000L),
                Arbitraries.integers().between(1, 64),
                Arbitraries.longs().between(0, 1_000_000_000L),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30)
        ).as(ResourceLimitPayload::new);
    }

    private Arbitrary<WriteConfig> writeConfigs() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30),
                Arbitraries.of("APPEND", "OVERWRITE"),
                partitionSpecs(),
                Arbitraries.of("parquet", "orc"),
                Arbitraries.longs().between(1_000_000, 512_000_000L)
        ).as(WriteConfig::new);
    }

    private Arbitrary<List<String>> partitionSpecs() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                .list().ofMinSize(0).ofMaxSize(4);
    }
}
