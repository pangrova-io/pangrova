package org.pangrova.server.worker.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.CdcSourceRange;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcWorkerRuntimeBootstrapTest {

    @Test
    void pinsAdmittedTopicAndAutoCommitWhilePassingSecurityProperties() {
        Map<String, String> configured = Map.of(
                "worker.cdc.kafka.properties.security.protocol", "SASL_SSL",
                "worker.cdc.kafka.properties.sasl.mechanism", "SCRAM-SHA-512",
                "worker.cdc.kafka.properties.topic", "attacker-topic");

        Map<String, String> properties = CdcWorkerRuntimeBootstrap.sourceProperties(
                request("orders"), "broker:9092", configured);

        assertThat(properties).containsEntry("bootstrap.servers", "broker:9092")
                .containsEntry("topic", "orders")
                .containsEntry("enable.auto.commit", "false")
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsKey("group.id");
    }

    @Test
    void rejectsTasksSpanningMultipleSourceTopics() {
        assertThatThrownBy(() -> CdcWorkerRuntimeBootstrap.sourceProperties(
                request("orders", "audit"), "broker:9092", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple source topics");
    }

    private static TaskRequest request(String... topics) {
        var binding = CdcIngestBinding.newBuilder()
                .setStreamId("stream-1").setTenantId("tenant-a")
                .setSourceOwnershipEpoch("epoch-1")
                .setSourceProvider("kafka").setFormatId("debezium-json");
        for (String topic : topics) {
            binding.addRanges(CdcSourceRange.newBuilder()
                    .setTopic(topic).setPartition(0)
                    .setStartOffset(10).setEndOffset(20));
        }
        return TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId("query").setTaskId("task")
                        .setPlanEpoch(1).setFencingToken(1).setControlEpoch(1))
                .setTaskKind(TaskKind.TASK_KIND_CDC_INGEST)
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId("tenant-a").setPrincipal("test-user"))
                .setCdcIngest(binding)
                .build();
    }
}
