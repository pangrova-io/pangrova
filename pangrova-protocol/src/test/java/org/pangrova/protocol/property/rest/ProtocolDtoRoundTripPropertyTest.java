package org.pangrova.protocol.property.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.data.DataFile;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.query.QueryStats;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskAdmitted;
import org.pangrova.protocol.http.TaskRejected;
import org.pangrova.protocol.http.TaskResultEndpoint;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusWire;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.OperatorStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.http.TaskStatusAck;
import org.pangrova.protocol.rest.task.TaskStatusWire;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying JSON round-trip for all REST DTOs in
 * {@code pangrova-protocol/rest/}. Each DTO is serialized to JSON and
 * deserialized back; the result must equal the original.
 *
 * <p><b>Property 31: Protocol DTO Round-Trip</b></p>
 * <p><b>Validates: Requirements 47.4, 50.5</b></p>
 */
class ProtocolDtoRoundTripPropertyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- config/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void writeConfigRoundTrip(
            @ForAll("writeConfigs") WriteConfig original) {
        assertRoundTrip(original, WriteConfig.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void outputConfigRoundTrip(
            @ForAll("outputConfigs") OutputConfig original) {
        assertRoundTrip(original, OutputConfig.class);
    }

    // --- data/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void dataFileRoundTrip(
            @ForAll("dataFiles") DataFile original) {
        assertRoundTrip(original, DataFile.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void writeResultRoundTrip(
            @ForAll("writeResults") WriteResultDescriptor original) {
        assertRoundTrip(original, WriteResultDescriptor.class);
    }

    // --- error/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void errorDetailRoundTrip(
            @ForAll("errorDetails") ErrorDetail original) {
        assertRoundTrip(original, ErrorDetail.class);
    }

    // --- query/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void queryStatsRoundTrip(
            @ForAll("queryStatsList") QueryStats original) {
        assertRoundTrip(original, QueryStats.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void submitQueryRequestRoundTrip(
            @ForAll("submitQueryRequests") SubmitQueryRequest original) {
        assertRoundTrip(original, SubmitQueryRequest.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void submitQueryResponseRoundTrip(
            @ForAll("submitQueryResponses") SubmitQueryResponse original) {
        assertRoundTrip(original, SubmitQueryResponse.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void queryStatusResponseRoundTrip(
            @ForAll("queryStatusResponses") QueryStatusResponse original) {
        assertRoundTrip(original, QueryStatusResponse.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void resultEndpointRoundTrip(
            @ForAll("resultEndpoints") ResultEndpoint original) {
        assertRoundTrip(original, ResultEndpoint.class);
    }

    // --- task/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void operatorStatsRoundTrip(
            @ForAll("operatorStatsList") OperatorStats original) throws Exception {
        var stats = NativeTaskStats.newBuilder().addOperatorStats(original).build();
        assertThat(roundTripTaskStats(stats).getOperatorStatsList()).containsExactly(original);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void exchangeSourceRoundTrip(
            @ForAll("exchangeSources") ExchangeSourceDescriptor original) {
        assertRoundTrip(original, ExchangeSourceDescriptor.class);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void taskStatsRoundTrip(
            @ForAll("taskStatsList") NativeTaskStats original) throws Exception {
        assertThat(roundTripTaskStats(original)).isEqualTo(original);
    }

    private static NativeTaskStats roundTripTaskStats(NativeTaskStats stats) throws Exception {
        var attempt = TaskAttemptId.newBuilder().setTaskId("task").setQueryOrJobId("query")
                .setPlanEpoch(1).setFencingToken(1).build();
        var progress = TaskStatusReport.fromAttempt(attempt, "COMPUTE", "worker", "RUNNING",
                stats, null, null, 1L, 1000, null);
        return TaskStatusWire.decode(TaskStatusWire.encode(progress)).supplementalStats();
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void submitTaskRequestRoundTrip(
            @ForAll("submitTaskRequests") SubmitTaskRequest original) throws Exception {
        assertThat(SubmitTaskRequest.decode(original.encode())).isEqualTo(original);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void taskAcknowledgementRoundTrip(
            @ForAll("taskAcknowledgements") TaskSubmitAck original) throws Exception {
        assertThat(TaskSubmitAck.parseFrom(original.toByteArray())).isEqualTo(original);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void taskStatusReportRoundTrip(
            @ForAll("taskStatusReports") TaskStatusReport original) throws Exception {
        assertThat(TaskStatusWire.decode(TaskStatusWire.encode(original))).isEqualTo(original);
    }

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void taskStatusReportResponseRoundTrip(
            @ForAll("taskStatusReportResponses") TaskStatusAck original) throws Exception {
        assertThat(TaskStatusAck.parseFrom(original.toByteArray())).isEqualTo(original);
    }

    // --- worker/ package ---

    @Property(tries = 10)
    @Tag("protocol-dto-round-trip")
    void workerRegistrationRequestRoundTrip(
            @ForAll("workerRegistrationRequests") WorkerRegistrationRequest original) {
        assertRoundTrip(original, WorkerRegistrationRequest.class);
    }

    // ========== Generators ==========

    // --- Leaf generators ---

    @Provide
    Arbitrary<StageResourceGrant> resourceGrants() {
        return Combinators.combine(
                Arbitraries.longs().between(1, 64L * 1024 * 1024 * 1024),
                Arbitraries.integers().between(1, 128),
                Arbitraries.longs().between(0, 128L * 1024 * 1024 * 1024)
        ).as((memory, cpu, spill) -> StageResourceGrant.newBuilder()
                .setGrantId("property-task:grant")
                .setNativeMemoryBytes(memory)
                .setCpuSlots(cpu)
                .setSpillBytes(spill)
                .build());
    }

    @Provide
    Arbitrary<OutputConfig> outputConfigs() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.longs().between(0, 1_000_000_000),
                Arbitraries.integers().between(1, 1000),
                Arbitraries.longs().between(1000, 600_000)
        ).as(OutputConfig::new);
    }

    @Provide
    Arbitrary<WriteConfig> writeConfigs() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.of("INSERT", "OVERWRITE", "MERGE"),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                        .list().ofMinSize(0).ofMaxSize(3),
                Arbitraries.of("PARQUET", "ORC"),
                Arbitraries.longs().between(1024, 256L * 1024 * 1024)
        ).as((target, mode, partitions, format, size) ->
                new WriteConfig(target, mode, partitions, format, size, "binding"));
    }

    @Provide
    Arbitrary<DataFile> dataFiles() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.longs().between(0, 10L * 1024 * 1024 * 1024),
                Arbitraries.longs().between(0, 100_000_000),
                Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(20)
        ).as(DataFile::new);
    }

    @Provide
    Arbitrary<WriteResultDescriptor> writeResults() {
        return Combinators.combine(
                dataFiles().list().ofMinSize(0).ofMaxSize(3),
                Arbitraries.longs().between(0, 100_000_000),
                Arbitraries.longs().between(0, 10L * 1024 * 1024 * 1024)
        ).as((files, rows, bytes) -> new WriteResultDescriptor(files, rows, bytes, new byte[0], java.util.List.of()));
    }

    @Provide
    Arbitrary<ErrorDetail> errorDetails() {
        return Combinators.combine(
                Arbitraries.of("QUERY_TIMEOUT", "MEMORY_EXCEEDED", "WORKER_LOST", "INTERNAL_ERROR"),
                Arbitraries.of("USER_ERROR", "INTERNAL_ERROR", "EXTERNAL_ERROR"),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50)
        ).as((code, type, msg) -> new ErrorDetail(code, type, msg));
    }

    @Provide
    Arbitrary<QueryStats> queryStatsList() {
        return Combinators.combine(
                Arbitraries.longs().between(0, 600_000),
                Arbitraries.longs().between(0, 60_000),
                Arbitraries.longs().between(0, 10_000_000)
        ).as(QueryStats::new);
    }

    @Provide
    Arbitrary<ResultEndpoint> resultEndpoints() {
        return resultEndpointArbitrary();
    }

    private static Arbitrary<ResultEndpoint> resultEndpointArbitrary() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.integers().between(1, 65535),
                nonBlankString(),
                Arbitraries.of("grpc", "grpc+tls")
        ).as(ResultEndpoint::new);
    }

    @Provide
    Arbitrary<SubmitQueryRequest> submitQueryRequests() {
        return Combinators.combine(
                nonBlankString(),
                sessionProperties()
        ).as(SubmitQueryRequest::new);
    }

    @Provide
    Arbitrary<SubmitQueryResponse> submitQueryResponses() {
        return queryStates().flatMap(state -> dispositionsForState(state).flatMap(disposition ->
                Combinators.combine(
                        nonBlankString(),
                        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                                .list().ofMinSize(0).ofMaxSize(5),
                        endpointsForDisposition(disposition)
                ).as((queryId, columns, endpoint) ->
                        new SubmitQueryResponse(queryId, state, disposition, endpoint, columns))));
    }

    @Provide
    Arbitrary<QueryStatusResponse> queryStatusResponses() {
        return queryStates().flatMap(state -> dispositionsForState(state).flatMap(disposition ->
                Combinators.combine(
                        nonBlankString(),
                        errorDetails().injectNull(0.5),
                        queryStatsList().injectNull(0.3),
                        endpointsForDisposition(disposition)
                ).as((queryId, error, stats, endpoint) ->
                        new QueryStatusResponse(queryId, state, error, stats, disposition, endpoint))));
    }

    private static Arbitrary<String> queryStates() {
        return Arbitraries.of("QUEUED", "RUNNING", "FINISHED", "FAILED", "CANCELLED");
    }

    private static Arbitrary<ResultDisposition> dispositionsForState(String state) {
        boolean terminal = "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
        return terminal ? Arbitraries.of(ResultDisposition.STREAM, ResultDisposition.NONE)
                : Arbitraries.of(ResultDisposition.PENDING, ResultDisposition.STREAM, ResultDisposition.NONE);
    }

    private static Arbitrary<ResultEndpoint> endpointsForDisposition(ResultDisposition disposition) {
        return disposition == ResultDisposition.STREAM ? resultEndpointArbitrary() : Arbitraries.just(null);
    }

    @Provide
    Arbitrary<OperatorStats> operatorStatsList() {
        return Combinators.combine(
                Arbitraries.of("TableScan", "Filter", "Project", "HashAggregate", "Sort"),
                Arbitraries.longs().between(0, 10_000_000),
                Arbitraries.longs().between(0, 10_000_000),
                Arbitraries.longs().between(0, 1_000_000_000),
                Arbitraries.longs().between(0, 1_000_000_000),
                Arbitraries.longs().between(0, 10_000_000_000L),
                Arbitraries.longs().between(0, 10_000_000_000L),
                nonBlankString()
        ).as((opType, inRows, outRows, inBytes, outBytes, cpuNs, wallNs, nodeId) ->
                OperatorStats.newBuilder().setOperatorType(opType).setPlanNodeId(nodeId)
                        .setInputRows(inRows).setOutputRows(outRows).setInputBytes(inBytes).setOutputBytes(outBytes)
                        .setCpuNanos(cpuNs).setWallNanos(wallNs).setMemoryBytes(inRows % 1_000_000_000)
                        .setPeakMemoryBytes(outRows % 2_000_000_000L).setSpillBytes(inBytes % 500_000_000)
                        .build());
    }

    @Provide
    Arbitrary<ExchangeSourceDescriptor> exchangeSources() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                Arbitraries.integers().between(1, 65535),
                Arbitraries.integers().between(0, 100),
                Arbitraries.of("SHUFFLE", "BROADCAST", "REPLICATE"),
                Arbitraries.longs().between(0, 1_000_000)
        ).as((taskId, host, port, partition, ignoredExchangeType, epoch) ->
                org.pangrova.protocol.rest.task.ProtocolTestFixtures.exchangeSource(
                        taskId, host, port, partition, "BLOCKING", epoch));
    }

    @Provide
    Arbitrary<NativeTaskStats> taskStatsList() {
        return operatorStatsList().list().ofMinSize(0).ofMaxSize(3)
                .flatMap(ops -> Arbitraries.longs().between(0, 10_000_000)
                        .map(rowsRead -> NativeTaskStats.newBuilder()
                                .setRowsRead(rowsRead).setBytesRead(rowsRead * 100)
                                .setRowsWritten(rowsRead / 2).setBytesWritten(rowsRead * 50)
                                .setElapsedTimeMs(rowsRead % 600_000).setCpuTimeMs(rowsRead % 300_000)
                                .setMemoryBytes(rowsRead % (8L * 1024 * 1024))
                                .setPeakMemoryBytes(rowsRead % (16L * 1024 * 1024))
                                .setSpillBytes(rowsRead % (32L * 1024 * 1024))
                                .setCacheHitBytes(rowsRead % (4L * 1024 * 1024))
                                .setCacheMissBytes(rowsRead % (4L * 1024 * 1024))
                                .addAllOperatorStats(ops).setBackpressureWaitMs(rowsRead % 10_000)
                                .setCacheHitCount(rowsRead % 1_000).setCacheMissCount(rowsRead % 1_000)
                                .setCacheEvictionCount(rowsRead % 1_000).setCacheAdmissionRejectionCount(rowsRead % 1_000)
                                .build()));
    }

    @Provide
    Arbitrary<SubmitTaskRequest> submitTaskRequests() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                Arbitraries.integers().between(0, 100),
                Arbitraries.integers().between(0, 100),
                Arbitraries.of("QUERY", "WRITE", "SHUFFLE")
        ).as(TaskIdTuple::new)
        .flatMap(tuple -> Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                        .list().ofMinSize(0).ofMaxSize(3),
                resourceGrants(),
                sessionProperties()
        ).as((splits, grant, sessionProperties) -> {
            TaskKind kind = switch (tuple.taskType) {
                case "WRITE" -> TaskKind.TASK_KIND_WRITE;
                case "SHUFFLE" -> TaskKind.TASK_KIND_SHUFFLE_READ;
                default -> TaskKind.TASK_KIND_READ;
            };
            var canonical = CanonicalTaskRequestTestFixture.builder(
                            tuple.id, tuple.queryId, kind,
                            "plan-fragment".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .setAttempt(CanonicalTaskRequestTestFixture.builder(
                                    tuple.id, tuple.queryId, kind, new byte[]{1})
                            .getAttempt().toBuilder()
                            .setStageId(tuple.stageId)
                            .setPartitionId(tuple.partitionId))
                    .setResourceGrant(grant)
                    .setExecutionOptions(TaskExecutionOptions.newBuilder()
                            .putAllSessionProperties(sessionProperties));
            splits.forEach(split -> canonical.addSplits(
                    DataSplit.newBuilder().setSplitId(split)));
            return SubmitTaskRequest.from(canonical.build());
        }));
    }

    @Provide
    Arbitrary<TaskSubmitAck> taskAcknowledgements() {
        return Combinators.combine(Arbitraries.of(true, false), nonBlankString(), nonBlankString(),
                resultEndpoints().injectNull(0.5))
                .as((accepted, reason, taskId, endpoint) -> {
                    var ack = TaskSubmitAck.newBuilder();
                    if (!accepted) {
                        return ack.setRejected(TaskRejected.newBuilder()
                                .setCode("BAD_REQUEST").setReason(reason)).build();
                    }
                    var admitted = TaskAdmitted.newBuilder().setTaskId(taskId);
                    if (endpoint != null) {
                        admitted.setResultEndpoint(TaskResultEndpoint.newBuilder()
                                .setHost(endpoint.host()).setPort(endpoint.port())
                                .setTicket(endpoint.ticket()).setProtocol(endpoint.protocol()));
                    }
                    return ack.setAdmitted(admitted).build();
                });
    }

    @Provide
    Arbitrary<TaskStatusReport> taskStatusReports() {
        return Combinators.combine(nonBlankString(), nonBlankString(), nonBlankString(),
                Arbitraries.of("RUNNING", "PENDING", "FLUSHING"),
                Arbitraries.longs().between(1, 1_000_000)).as((id, query, worker, state, sequence) -> {
                    var attempt = org.pangrova.protocol.v2.TaskAttemptId.newBuilder().setTaskId(id)
                            .setQueryOrJobId(query).setPlanEpoch(1).setFencingToken(1).build();
                    return TaskStatusReport.fromAttempt(attempt, "COMPUTE", worker, state,
                            null, null, null, sequence, 1_700_000_000_000L, null);
                });
    }

    @Provide
    Arbitrary<TaskStatusAck> taskStatusReportResponses() {
        return Combinators.combine(nonBlankString(), Arbitraries.longs().between(1, 1_000_000),
                Arbitraries.of(TaskStatusAck.Directive.CONTINUE, TaskStatusAck.Directive.STALE_REJECTED,
                        TaskStatusAck.Directive.RETIRED_RESULT_NOTICE))
                .as((taskId, sequence, directive) -> TaskStatusAck.newBuilder().setTaskId(taskId)
                        .setSequence(sequence).setDirective(directive).build());
    }

    @Provide
    Arbitrary<WorkerRegistrationRequest> workerRegistrationRequests() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                Arbitraries.integers().between(1, 65535),
                Arbitraries.integers().between(1, 65535),
                Arbitraries.integers().between(0, 65535)
        ).as(WorkerBaseTuple::new)
        .flatMap(tuple -> Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).injectNull(0.3),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                        .list().ofMinSize(0).ofMaxSize(3)
        ).as((rackId, dataLocality) ->
                new WorkerRegistrationRequest(
                        tuple.workerId, tuple.httpHost, tuple.httpPort,
                        tuple.transportPort, tuple.resultServerPort,
                        2, rackId, dataLocality, null,
                        org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture
                                .encodedInventory(), new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"))));
    }

    // ========== Intermediate tuple records for flatMap ==========

    private record TaskIdTuple(
            String id, String queryId, int stageId,
            int partitionId, String taskType) {}

    private record WorkerBaseTuple(
            String workerId, String httpHost, int httpPort,
            int transportPort, int resultServerPort) {}

    // ========== Helpers ==========

    private static Arbitrary<String> nonBlankString() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    private static Arbitrary<Map<String, String>> sessionProperties() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
        ).ofMinSize(0).ofMaxSize(3);
    }

    /**
     * Core round-trip assertion: serialize to JSON, deserialize back,
     * verify equality with the original instance.
     */
    private static <T> void assertRoundTrip(T original, Class<T> type) {
        try {
            String json = MAPPER.writeValueAsString(original);
            T deserialized = MAPPER.readValue(json, type);
            assertThat(deserialized)
                    .as("Round-trip for %s: original → JSON → deserialized must be equal",
                            type.getSimpleName())
                    .isEqualTo(original);
        } catch (JsonProcessingException e) {
            throw new AssertionError(
                    "JSON round-trip failed for " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }
}
