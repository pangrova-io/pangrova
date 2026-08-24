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
import org.pangrova.protocol.rest.config.NativeInitConfig;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.data.DataFile;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.QueryStats;
import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryRequest;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.task.OperatorStats;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.SubmitTaskResponse;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusReportResponse;
import org.pangrova.protocol.rest.task.TaskStatusResponse;
import org.pangrova.protocol.rest.task.UpdateExchangeSourcesRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;

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
    @Tag("Property 31: Protocol DTO Round-Trip")
    void resourceLimitRoundTrip(
            @ForAll("resourceLimits") ResourceLimitPayload original) {
        assertRoundTrip(original, ResourceLimitPayload.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void writeConfigRoundTrip(
            @ForAll("writeConfigs") WriteConfig original) {
        assertRoundTrip(original, WriteConfig.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void outputConfigRoundTrip(
            @ForAll("outputConfigs") OutputConfig original) {
        assertRoundTrip(original, OutputConfig.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void nativeInitConfigRoundTrip(
            @ForAll("nativeInitConfigs") NativeInitConfig original) {
        assertRoundTrip(original, NativeInitConfig.class);
    }

    // --- data/ package ---

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void dataFileRoundTrip(
            @ForAll("dataFiles") DataFile original) {
        assertRoundTrip(original, DataFile.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void writeResultRoundTrip(
            @ForAll("writeResults") WriteResultDescriptor original) {
        assertRoundTrip(original, WriteResultDescriptor.class);
    }

    // --- error/ package ---

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void errorDetailRoundTrip(
            @ForAll("errorDetails") ErrorDetail original) {
        assertRoundTrip(original, ErrorDetail.class);
    }

    // --- query/ package ---

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void queryStatsRoundTrip(
            @ForAll("queryStatsList") QueryStats original) {
        assertRoundTrip(original, QueryStats.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void submitQueryRequestRoundTrip(
            @ForAll("submitQueryRequests") SubmitQueryRequest original) {
        assertRoundTrip(original, SubmitQueryRequest.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void submitQueryResponseRoundTrip(
            @ForAll("submitQueryResponses") SubmitQueryResponse original) {
        assertRoundTrip(original, SubmitQueryResponse.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void queryStatusResponseRoundTrip(
            @ForAll("queryStatusResponses") QueryStatusResponse original) {
        assertRoundTrip(original, QueryStatusResponse.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void resultEndpointRoundTrip(
            @ForAll("resultEndpoints") ResultEndpoint original) {
        assertRoundTrip(original, ResultEndpoint.class);
    }

    // --- task/ package ---

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void operatorStatsRoundTrip(
            @ForAll("operatorStatsList") OperatorStats original) {
        assertRoundTrip(original, OperatorStats.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void exchangeSourceRoundTrip(
            @ForAll("exchangeSources") ExchangeSourceDescriptor original) {
        assertRoundTrip(original, ExchangeSourceDescriptor.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void taskStatsRoundTrip(
            @ForAll("taskStatsList") TaskStats original) {
        assertRoundTrip(original, TaskStats.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void submitTaskRequestRoundTrip(
            @ForAll("submitTaskRequests") SubmitTaskRequest original) {
        assertRoundTrip(original, SubmitTaskRequest.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void submitTaskResponseRoundTrip(
            @ForAll("submitTaskResponses") SubmitTaskResponse original) {
        assertRoundTrip(original, SubmitTaskResponse.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void taskStatusReportRoundTrip(
            @ForAll("taskStatusReports") TaskStatusReport original) {
        assertRoundTrip(original, TaskStatusReport.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void taskStatusReportResponseRoundTrip(
            @ForAll("taskStatusReportResponses") TaskStatusReportResponse original) {
        assertRoundTrip(original, TaskStatusReportResponse.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void taskStatusResponseRoundTrip(
            @ForAll("taskStatusResponses") TaskStatusResponse original) {
        assertRoundTrip(original, TaskStatusResponse.class);
    }

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void updateExchangeSourcesRequestRoundTrip(
            @ForAll("updateExchangeSourcesRequests") UpdateExchangeSourcesRequest original) {
        assertRoundTrip(original, UpdateExchangeSourcesRequest.class);
    }

    // --- worker/ package ---

    @Property(tries = 10)
    @Tag("Property 31: Protocol DTO Round-Trip")
    void workerRegistrationRequestRoundTrip(
            @ForAll("workerRegistrationRequests") WorkerRegistrationRequest original) {
        assertRoundTrip(original, WorkerRegistrationRequest.class);
    }

    // ========== Generators ==========

    // --- Leaf generators ---

    @Provide
    Arbitrary<ResourceLimitPayload> resourceLimits() {
        return Combinators.combine(
                Arbitraries.longs().between(0, 64L * 1024 * 1024 * 1024),
                Arbitraries.integers().between(1, 128),
                Arbitraries.longs().between(0, 128L * 1024 * 1024 * 1024),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30)
        ).as(ResourceLimitPayload::new);
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
        ).as(WriteConfig::new);
    }

    @Provide
    Arbitrary<NativeInitConfig> nativeInitConfigs() {
        return Combinators.combine(
                Arbitraries.longs().between(1, 64L * 1024 * 1024 * 1024),
                Arbitraries.integers().between(1, 64),
                Arbitraries.of(true, false),
                Arbitraries.longs().between(0, 32L * 1024 * 1024 * 1024),
                Arbitraries.longs().between(0, 200L * 1024 * 1024 * 1024),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30)
        ).as(NativeInitConfig::new);
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
        ).as((files, rows, bytes) -> new WriteResultDescriptor(files, rows, bytes, new byte[0]));
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
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.of("QUEUED", "RUNNING", "FINISHED"),
                resultEndpoints().injectNull(0.3),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                        .list().ofMinSize(0).ofMaxSize(5)
        ).as(SubmitQueryResponse::new);
    }

    @Provide
    Arbitrary<QueryStatusResponse> queryStatusResponses() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.of("QUEUED", "RUNNING", "FINISHED", "FAILED"),
                errorDetails().injectNull(0.5),
                queryStatsList().injectNull(0.3),
                resultEndpoints().injectNull(0.5)
        ).as(QueryStatusResponse::new);
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
                new OperatorStats(opType, inRows, outRows, inBytes, outBytes,
                        cpuNs, wallNs, nodeId,
                        Math.abs(inRows % (1_000_000_000)),
                        Math.abs(outRows % (2_000_000_000L)),
                        Math.abs(inBytes % 500_000_000)));
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
        ).as(ExchangeSourceDescriptor::new);
    }

    @Provide
    Arbitrary<TaskStats> taskStatsList() {
        return operatorStatsList().list().ofMinSize(0).ofMaxSize(3)
                .flatMap(ops -> Arbitraries.longs().between(0, 10_000_000)
                        .map(rowsRead -> new TaskStats(
                                rowsRead,
                                rowsRead * 100,
                                rowsRead / 2,
                                rowsRead * 50,
                                Math.abs(rowsRead % 600_000),
                                Math.abs(rowsRead % 300_000),
                                Math.abs(rowsRead % (8L * 1024 * 1024)),
                                Math.abs(rowsRead % (16L * 1024 * 1024)),
                                Math.abs(rowsRead % (32L * 1024 * 1024)),
                                Math.abs(rowsRead % (4L * 1024 * 1024)),
                                Math.abs(rowsRead % (4L * 1024 * 1024)),
                                Math.abs(rowsRead % 1_000_000),
                                Math.abs(rowsRead % 2_000_000),
                                ops)));
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
                exchangeSources().list().ofMinSize(0).ofMaxSize(2),
                resourceLimits(),
                sessionProperties(),
                outputConfigs().injectNull(0.3),
                writeConfigs().injectNull(0.5)
        ).as((splits, exSources, rl, sp, oc, wc) ->
                new SubmitTaskRequest(
                        tuple.id, tuple.queryId, tuple.stageId,
                        tuple.partitionId, tuple.taskType,
                        "plan-fragment-base64",
                        splits, exSources, rl, sp, oc, wc,
                        null, null, null, null, null, null, null)));
    }

    @Provide
    Arbitrary<SubmitTaskResponse> submitTaskResponses() {
        return Combinators.combine(
                Arbitraries.of(true, false),
                Arbitraries.strings().alpha().ofMinLength(0).ofMaxLength(30).injectNull(0.5),
                nonBlankString(),
                resultEndpoints().injectNull(0.5)
        ).as(SubmitTaskResponse::new);
    }

    @Provide
    Arbitrary<TaskStatusReport> taskStatusReports() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                Arbitraries.integers().between(0, 100),
                Arbitraries.integers().between(0, 100),
                Arbitraries.of("QUERY", "WRITE", "SHUFFLE")
        ).as(TaskIdTuple::new)
        .flatMap(tuple -> Combinators.combine(
                nonBlankString(),
                Arbitraries.of("RUNNING", "FINISHED", "FAILED", "CANCELLED"),
                taskStatsList().injectNull(0.2),
                errorDetails().injectNull(0.7),
                writeResults().injectNull(0.7),
                Arbitraries.longs().between(0, 1_000_000),
                Arbitraries.longs().between(1_700_000_000_000L, 1_800_000_000_000L)
        ).as((workerId, state, stats, error, wr, seq, ts) ->
                new TaskStatusReport(
                        tuple.id, tuple.queryId, tuple.stageId,
                        tuple.partitionId, tuple.taskType,
                        workerId, state, stats, error, wr, seq, ts)));
    }

    @Provide
    Arbitrary<TaskStatusReportResponse> taskStatusReportResponses() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.of("ACK", "CANCEL", "ABORT").injectNull(0.3)
        ).as(TaskStatusReportResponse::new);
    }

    @Provide
    Arbitrary<TaskStatusResponse> taskStatusResponses() {
        return Combinators.combine(
                nonBlankString(),
                Arbitraries.of("RUNNING", "FINISHED", "FAILED", "CANCELLED"),
                taskStatsList().injectNull(0.3),
                errorDetails().injectNull(0.5)
        ).as(TaskStatusResponse::new);
    }

    @Provide
    Arbitrary<UpdateExchangeSourcesRequest> updateExchangeSourcesRequests() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                exchangeSources().list().ofMinSize(0).ofMaxSize(3),
                Arbitraries.of("NEW_STAGE_OUTPUT", "RETRY", "REBALANCE")
        ).as(UpdateExchangeSourcesRequest::new);
    }

    @Provide
    Arbitrary<WorkerRegistrationRequest> workerRegistrationRequests() {
        return Combinators.combine(
                nonBlankString(),
                nonBlankString(),
                Arbitraries.integers().between(1, 65535),
                Arbitraries.integers().between(1, 65535),
                Arbitraries.integers().between(0, 65535),
                Arbitraries.integers().between(1, 10),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                        .list().ofMinSize(0).ofMaxSize(3)
        ).as(WorkerBaseTuple::new)
        .flatMap(tuple -> Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).injectNull(0.3),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                        .list().ofMinSize(0).ofMaxSize(3)
        ).as((rackId, dataLocality) ->
                new WorkerRegistrationRequest(
                        tuple.workerId, tuple.httpHost, tuple.httpPort,
                        tuple.transportPort, tuple.resultServerPort,
                        tuple.protocolVersion, tuple.capabilities,
                        rackId, dataLocality, null)));
    }

    // ========== Intermediate tuple records for flatMap ==========

    private record TaskIdTuple(
            String id, String queryId, int stageId,
            int partitionId, String taskType) {}

    private record WorkerBaseTuple(
            String workerId, String httpHost, int httpPort,
            int transportPort, int resultServerPort,
            int protocolVersion, List<String> capabilities) {}

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
