package org.pangrova.protocol.rest;

import org.pangrova.protocol.rest.config.NativeInitConfig;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.data.DataFile;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.SubmitQueryRequest;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.UpdateExchangeSourcesRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies all REST DTOs with collection fields perform defensive copying,
 * ensuring true immutability per Requirements 2.1, 2.3, 2.7.
 */
class RestDtoImmutabilityTest {

    @Test
    void writeConfigPartitionSpecIsDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("region", "date"));
        WriteConfig wc = new WriteConfig("t", "APPEND", mutable, "parquet", 128_000_000);

        mutable.add("injected");

        assertThat(wc.partitionSpec()).hasSize(2);
        assertThatThrownBy(() -> wc.partitionSpec().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writeConfigNullPartitionSpecBecomesEmptyList() {
        WriteConfig wc = new WriteConfig("t", "APPEND", null, "parquet", 128_000_000);
        assertThat(wc.partitionSpec()).isEmpty();
    }

    @Test
    void writeResultDataFilesIsDefensivelyCopied() {
        List<DataFile> mutable = new ArrayList<>(List.of(
                new DataFile("/a.parquet", 100, 10, "p=1")));
        WriteResultDescriptor wr = new WriteResultDescriptor(mutable, 10, 100, new byte[0]);

        mutable.add(new DataFile("/b.parquet", 200, 20, "p=2"));

        assertThat(wr.dataFiles()).hasSize(1);
        assertThatThrownBy(() -> wr.dataFiles().add(new DataFile("/c.parquet", 0, 0, "")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writeResultNullDataFilesBecomesEmptyList() {
        WriteResultDescriptor wr = new WriteResultDescriptor(null, 0, 0, new byte[0]);
        assertThat(wr.dataFiles()).isEmpty();
    }

    @Test
    void submitQueryRequestSessionPropertiesIsDefensivelyCopied() {
        Map<String, String> mutable = new HashMap<>(Map.of("k", "v"));
        SubmitQueryRequest req = new SubmitQueryRequest("SELECT 1", mutable);

        mutable.put("injected", "bad");

        assertThat(req.sessionProperties()).hasSize(1);
        assertThatThrownBy(() -> req.sessionProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void submitQueryRequestNullSessionPropertiesBecomesEmptyMap() {
        SubmitQueryRequest req = new SubmitQueryRequest("SELECT 1", null);
        assertThat(req.sessionProperties()).isEmpty();
    }

    @Test
    void submitQueryResponseColumnNamesIsDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("col1", "col2"));
        SubmitQueryResponse resp = new SubmitQueryResponse(
                "q-1", "RUNNING", null, mutable);

        mutable.add("injected");

        assertThat(resp.columnNames()).hasSize(2);
        assertThatThrownBy(() -> resp.columnNames().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void submitQueryResponseNullColumnNamesBecomesEmptyList() {
        SubmitQueryResponse resp = new SubmitQueryResponse(
                "q-1", "RUNNING", null, null);
        assertThat(resp.columnNames()).isEmpty();
    }

    @Test
    void submitTaskRequestCollectionsAreDefensivelyCopied() {
        List<String> splits = new ArrayList<>(List.of("s1"));
        List<ExchangeSourceDescriptor> sources = new ArrayList<>(List.of(
                new ExchangeSourceDescriptor("t1", "host", 9090, 0, "SHUFFLE", 1)));
        Map<String, String> props = new HashMap<>(Map.of("k", "v"));

        SubmitTaskRequest req = new SubmitTaskRequest(
                "task-1", "q-1", 0, 0, "COMPUTE", "plan",
                splits, sources, null, props, null, null,
                null, null, null, null, null, null, null);

        splits.add("injected");
        sources.add(new ExchangeSourceDescriptor("t2", "h2", 9091, 1, "SHUFFLE", 2));
        props.put("injected", "bad");

        assertThat(req.splits()).hasSize(1);
        assertThat(req.exchangeSources()).hasSize(1);
        assertThat(req.sessionProperties()).hasSize(1);

        assertThatThrownBy(() -> req.splits().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> req.exchangeSources().add(
                new ExchangeSourceDescriptor("t3", "h3", 9092, 0, "SHUFFLE", 0)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> req.sessionProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void submitTaskRequestNullCollectionsBecomeEmpty() {
        SubmitTaskRequest req = new SubmitTaskRequest(
                "task-1", "q-1", 0, 0, null, null,
                null, null, null, null, null, null,
                null, null, null, null, null, null, null);

        assertThat(req.splits()).isEmpty();
        assertThat(req.exchangeSources()).isEmpty();
        assertThat(req.sessionProperties()).isEmpty();
    }

    @Test
    void updateExchangeSourcesRequestIsDefensivelyCopied() {
        List<ExchangeSourceDescriptor> mutable = new ArrayList<>(List.of(
                new ExchangeSourceDescriptor("t1", "host", 9090, 0, "SHUFFLE", 1)));
        UpdateExchangeSourcesRequest req = new UpdateExchangeSourcesRequest(
                "task-1", "q-1", mutable, "stage-complete");

        mutable.add(new ExchangeSourceDescriptor("t2", "h2", 9091, 1, "SHUFFLE", 2));

        assertThat(req.exchangeSources()).hasSize(1);
        assertThatThrownBy(() -> req.exchangeSources().add(
                new ExchangeSourceDescriptor("t3", "h3", 9092, 0, "SHUFFLE", 0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void updateExchangeSourcesRequestNullBecomesEmptyList() {
        UpdateExchangeSourcesRequest req = new UpdateExchangeSourcesRequest(
                "task-1", "q-1", null, "reason");
        assertThat(req.exchangeSources()).isEmpty();
    }

    @Test
    void workerRegistrationRequestCapabilitiesAlreadyDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("cap1"));
        WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                "w-1", "localhost", 8080, 9090, 0, 1, mutable, null, null, null);

        mutable.add("injected");

        assertThat(req.capabilities()).hasSize(1);
        assertThatThrownBy(() -> req.capabilities().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void allRestDtosAreRecords() {
        assertThat(DataFile.class.isRecord()).isTrue();
        assertThat(ErrorDetail.class.isRecord()).isTrue();
        assertThat(NativeInitConfig.class.isRecord()).isTrue();
        assertThat(OutputConfig.class.isRecord()).isTrue();
        assertThat(ResourceLimitPayload.class.isRecord()).isTrue();
        assertThat(WorkerRegistrationRequest.class.isRecord()).isTrue();
        assertThat(WriteConfig.class.isRecord()).isTrue();
        assertThat(WriteResultDescriptor.class.isRecord()).isTrue();
    }
}
