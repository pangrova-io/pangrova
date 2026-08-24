package org.pangrova.coordinator.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.TaskDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubmitTaskRequestConverter}.
 * Validates DTO→SPI conversion and result endpoint construction.
 */
class SubmitTaskRequestConverterTest {

    @Test
    void toTaskDescriptorConvertsAllFields() {
        String planB64 = Base64.getEncoder().encodeToString("plan-bytes".getBytes());
        String splitB64 = Base64.getEncoder().encodeToString("split-1".getBytes());
        SubmitTaskRequest request = createRequest("q-1", planB64, List.of(splitB64));

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.queryId().toString()).isEqualTo("q-1");
        assertThat(descriptor.splits()).hasSize(1);
        assertThat(descriptor.sessionProperties()).containsEntry("key", "value");
    }

    @Test
    void toTaskDescriptorHandlesNullPlanFragment() {
        SubmitTaskRequest request = createRequest("q-2", null, List.of());

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.planFragment()).isNotNull();
    }

    @Test
    void toTaskDescriptorHandlesNullSplits() {
        SubmitTaskRequest request = createRequest("q-3", "cGxhbg==", null);

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.splits()).isEmpty();
    }

    @Test
    void toTaskDescriptorHandlesNullSessionProperties() {
        SubmitTaskRequest request = new SubmitTaskRequest(
                "t-1", "q-4", 0, 0, "COMPUTE",
                "cGxhbg==", List.of(), List.of(), null, null, null, null,
                null, null, null, null, null, null, null);

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.sessionProperties()).isEmpty();
    }

    @Test
    void buildResultEndpointUsesWorkerTransportPort() {
        WorkerInfo worker = createWorker();

        ResultEndpoint endpoint = SubmitTaskRequestConverter.buildResultEndpoint(worker, "task-42");

        assertThat(endpoint.host()).isEqualTo("worker-host");
        assertThat(endpoint.port()).isEqualTo(8082);
        assertThat(endpoint.ticket()).isEqualTo("task-42");
        assertThat(endpoint.protocol()).isEqualTo("grpc-transport");
    }

    @Test
    void serializedPlanFragmentProvidesPlanBytes() {
        byte[] raw = "test-plan".getBytes();
        var fragment = new org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter.SerializedPlanFragment(raw);

        assertThat(fragment.planBytes()).isEqualTo(raw);
        assertThat(fragment.getId().toString()).isEqualTo("serialized-fragment");
        assertThat(fragment.getChildFragmentIds()).isEmpty();
    }

    private static SubmitTaskRequest createRequest(
            String queryId, String planFragment, List<String> splits) {
        return new SubmitTaskRequest(
                "task-1", queryId, 0, 0, "COMPUTE",
                planFragment, splits, List.of(), null,
                Map.of("key", "value"), null, null,
                null, null, null, null, null, null, null);
    }

    private static WorkerInfo createWorker() {
        return TestWorkerInfoBuilder.active("w-1", "worker-host", 8080, 8082);
    }
}
