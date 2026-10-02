package org.pangrova.protocol.rest;

import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.data.DataFile;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.SubmitQueryRequest;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.TaskKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies all REST DTOs with collection fields perform defensive copying,
 * ensuring true immutability per Requirements 2.1, 2.3, 2.7.
 */
class RestDtoImmutabilityTest {

    @Test
    void catalogDeploymentCollectionsAreDefensivelyCopied() {
        Map<String, String> properties = new HashMap<>(Map.of("endpoint", "original"));
        Set<String> tenants = new HashSet<>(Set.of("tenant-a"));
        var response = new CatalogDeploymentResponse(
                "catalog", "provider", "incarnation", 1L, properties, tenants);

        properties.put("endpoint", "changed");
        tenants.add("tenant-b");

        assertThat(response.properties()).containsEntry("endpoint", "original");
        assertThat(response.allowedTenants()).containsExactly("tenant-a");
        assertThatThrownBy(() -> response.allowedTenants().add("tenant-c"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writeConfigPartitionSpecIsDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("region", "date"));
        WriteConfig wc = new WriteConfig("t", "APPEND", mutable, "parquet", 128_000_000, "binding");

        mutable.add("injected");

        assertThat(wc.partitionSpec()).hasSize(2);
        assertThatThrownBy(() -> wc.partitionSpec().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writeConfigNullPartitionSpecBecomesEmptyList() {
        WriteConfig wc = new WriteConfig("t", "APPEND", null, "parquet", 128_000_000, "binding");
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
    void submitTaskRequestReturnsDefensiveCanonicalByteCopies() {
        SubmitTaskRequest req = SubmitTaskRequest.from(
                CanonicalTaskRequestTestFixture.builder(
                        "task-1", "q-1", TaskKind.TASK_KIND_READ,
                        new byte[]{1, 2, 3}).build());

        byte[] first = req.canonicalBytes();
        byte original = first[0];
        first[0] ^= 0x7f;

        assertThat(req.canonicalBytes()[0]).isEqualTo(original);
    }

    @Test
    void submitTaskRequestCanonicalProtoIsImmutable() {
        SubmitTaskRequest req = SubmitTaskRequest.from(
                CanonicalTaskRequestTestFixture.builder(
                        "task-1", "q-1", TaskKind.TASK_KIND_READ,
                        new byte[]{1}).build());

        assertThat(req.taskRequest().getSplitsList()).isEmpty();
        assertThatThrownBy(() -> req.taskRequest().getSplitsList()
                .add(DataSplit.getDefaultInstance()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void workerRegistrationRequestDataLocalityIsDefensivelyCopied() {
        List<String> mutable = new ArrayList<>(List.of("file-1"));
        WorkerRegistrationRequest req = new WorkerRegistrationRequest(
                "w-1", "localhost", 8080, 9090, 0, 2,
                null, mutable, null,
                org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture
                        .encodedInventory());

        mutable.add("injected");

        assertThat(req.dataLocality()).containsExactly("file-1");
        assertThatThrownBy(() -> req.dataLocality().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void allRestDtosAreRecords() {
        assertThat(DataFile.class.isRecord()).isTrue();
        assertThat(ErrorDetail.class.isRecord()).isTrue();
        assertThat(OutputConfig.class.isRecord()).isTrue();
        assertThat(WorkerRegistrationRequest.class.isRecord()).isTrue();
        assertThat(WriteConfig.class.isRecord()).isTrue();
        assertThat(WriteResultDescriptor.class.isRecord()).isTrue();
    }
}
