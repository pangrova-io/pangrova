package org.pangrova.execution.core.binding;


import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.StageResourceGrant;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TaskRequestFactoryResourceGrantTest {

    @Test
    void sumsEveryOperatorProviderBudgetIntoCanonicalTaskGrant() {
        StageResourceGrant template = StageResourceGrant.newBuilder()
                .setGrantId("template")
                .setCpuSlots(4)
                .setNativeMemoryBytes(1024)
                .setObjectStoreReadBytes(1024)
                .setObjectStoreWriteBytes(1024)
                .setMediaBytes(64)
                .build();

        StageResourceGrant grant = TaskRequestFactory.bindResourceGrant(
                "task-1",
                template,
                List.of(operator("ai-1", 3, 30, 12),
                        operator("ai-2", 5, 50, 20)));

        assertThat(grant.getGrantId()).isEqualTo("task-1:grant");
        assertThat(grant.getProviderItems()).isEqualTo(8);
        assertThat(grant.getProviderAttempts()).isEqualTo(10);
        assertThat(grant.getProviderRequestBytes()).isEqualTo(80);
        assertThat(grant.getProviderResponseBytes()).isEqualTo(32);
        assertThat(grant.getProviderConcurrency()).isEqualTo(4);
        assertThat(grant.getMediaBytes()).isEqualTo(64);
    }

    private static OperatorBinding operator(
            String id, long items, long requestBytes, long responseBytes) {
        return OperatorBinding.newBuilder()
                .setPlanNodeId(id)
                .setResources(OperatorResourceRequirement.newBuilder()
                        .setProviderItems(items)
                        .setProviderAttempts(items + 1)
                        .setProviderRequestBytes(requestBytes)
                        .setProviderResponseBytes(responseBytes)
                        .setProviderConcurrency(2))
                .build();
    }
}
