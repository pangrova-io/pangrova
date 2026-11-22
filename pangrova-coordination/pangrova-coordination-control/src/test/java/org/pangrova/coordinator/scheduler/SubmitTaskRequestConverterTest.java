package org.pangrova.coordinator.scheduler;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubmitTaskRequestConverter}.
 * Validates DTO→SPI conversion. Producer endpoint oracles live with Worker signing.
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
        SubmitTaskRequest request =
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        "t-1", "q-4", 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        TaskDescriptor descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.sessionProperties()).isEmpty();
    }

    @Test
    void serializedPlanFragmentProvidesPlanBytes() {
        byte[] raw = "test-plan".getBytes();
        var fragment = new org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter.SerializedPlanFragment(
                com.google.protobuf.ByteString.copyFrom(raw));

        assertThat(fragment.planBytes()).isEqualTo(raw);
        assertThat(fragment.getId().toString()).isEqualTo("serialized-fragment");
        assertThat(fragment.getChildFragmentIds()).isEmpty();
    }

    private static SubmitTaskRequest createRequest(
            String queryId, String planFragment, List<String> splits) {
        byte[] plan = planFragment == null
                ? new byte[0] : Base64.getDecoder().decode(planFragment);
        var canonical = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.builder(
                        "task-1", queryId, 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ, plan)
                .setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions
                        .newBuilder().putAllSessionProperties(Map.of("key", "value")));
        if (splits != null) {
            for (int index = 0; index < splits.size(); index++) {
                canonical.addSplits(org.pangrova.protocol.v2.DataSplit.newBuilder()
                        .setSplitId("split-" + index));
            }
        }
        return SubmitTaskRequest.from(canonical.build());
    }

}
