package org.pangrova.worker.result;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ResultReadGrant;
import org.pangrova.protocol.v2.ResultReadScope;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultReadTicketsTest {
    private final byte[] key = "producer-transport-key-32-bytes!!".getBytes(StandardCharsets.UTF_8);

    private TaskRequest request() {
        return CanonicalTaskRequestTestFixture.builder("task-42", "query", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[] {1})
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId("result-test").setCpuSlots(1).setNativeMemoryBytes(1048576)
                        .setShuffleBufferBytes(1048576))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setDirectResultOutput(true)).build();
    }

    @Test
    void producerEndpointPreservesAddressAndAuthenticatesTheCompleteAuthority() throws Exception {
        TaskRequest request = request();
        var endpoint = new ResultReadTickets(key).endpoint(request, "worker-host", 8082, "grpc");
        assertThat(endpoint.host()).isEqualTo("worker-host");
        assertThat(endpoint.port()).isEqualTo(8082);
        assertThat(endpoint.protocol()).isEqualTo("grpc");
        assertThat(endpoint.ticket()).isNotEqualTo(request.getAttempt().getTaskId());
        String[] parts = endpoint.ticket().split("\\|", -1);
        assertThat(parts).hasSize(3);
        assertThat(parts[0]).startsWith("pangrova.result.read.v1:");
        ResultReadGrant grant = ResultReadGrant.parseFrom(Base64.getDecoder().decode(
                parts[0].substring("pangrova.result.read.v1:".length())));
        assertThat(grant.getSecurity()).isEqualTo(request.getSecurity());
        assertThat(grant.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(grant.getScope()).isEqualTo(ResultReadScope.RESULT_READ_SCOPE_RESULT_STREAM);
        assertThat(grant.getExpiresAtEpochMs()).isEqualTo(request.getDeadlineEpochMs());
        assertThat(parts[1]).isEqualTo(Long.toString(request.getDeadlineEpochMs()));
        Mac actual = Mac.getInstance("HmacSHA256");
        actual.init(new SecretKeySpec(key, "HmacSHA256"));
        assertThat(parts[2]).isEqualTo(HexFormat.of().formatHex(actual.doFinal(
                (parts[0] + "|" + parts[1]).getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void refusesAbsentKeyExpiredAttemptAndTasksWithNoResultStream() {
        assertThatThrownBy(() -> new ResultReadTickets(new byte[31])).isInstanceOf(IllegalArgumentException.class);
        var tickets = new ResultReadTickets(key);
        assertThatThrownBy(() -> tickets.issue(request().toBuilder().setDeadlineEpochMs(1).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tickets.issue(request().toBuilder()
                .setExecutionOptions(TaskExecutionOptions.getDefaultInstance()).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tickets.issue(request().toBuilder()
                .setResourceGrant(request().getResourceGrant().toBuilder().clearShuffleBufferBytes()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Serialized output requires a positive serialized-buffer grant");
    }
}
