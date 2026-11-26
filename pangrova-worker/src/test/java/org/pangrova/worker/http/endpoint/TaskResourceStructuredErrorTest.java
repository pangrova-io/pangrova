package org.pangrova.worker.http.endpoint;

import com.google.protobuf.CodedOutputStream;
import jakarta.ws.rs.core.Response;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskSubmission;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests structured task admission errors.
 */
class TaskResourceStructuredErrorTest {

    @Test
    @DisplayName("submitTask returns BAD_REQUEST for null request body")
    void submitTaskReturnsBadRequestForNull() throws Exception {
        TaskResource resource = buildResource();

        Response response = resource.submitTask(null, null).toCompletableFuture().join();

        assertThat(response.getStatus()).isEqualTo(400);
        var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    @DisplayName("submitTask returns a rejected acknowledgement on conversion failure")
    void submitTaskReturnsStructuredErrorOnConversionFailure() throws Exception {
        TaskResource resource = buildResource();

        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "task-1", "query-1", 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .clearSubstraitPlanChecksum()
                .build();
        SubmitTaskRequest request = SubmitTaskRequest.from(canonical);

        Response response = resource.submitTask(new ByteArrayInputStream(request.encode()), null).toCompletableFuture().join();

        assertThat(response.getStatus()).isEqualTo(400);
        var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
        assertThat(ack.hasRejected()).isTrue();
        assertThat(ack.getRejected().getReason()).contains("checksum is invalid");
    }

    @Test
    void unknownLengthBodyIsRejectedAtTheFirstByteBeyondTheLimit() throws Exception {
        var prefix = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(prefix);
        int limit = SubmitTaskRequest.MAX_HTTP_BODY_BYTES;
        output.writeTag(TaskSubmission.TASK_FIELD_NUMBER, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED);
        output.writeUInt32NoTag(limit + 128);
        output.flush();
        // Repeat one legal scalar field to exercise the wire limit without allocating a large payload.
        var scalar = new ByteArrayOutputStream();
        output = CodedOutputStream.newInstance(scalar);
        output.writeEnum(TaskRequest.TASK_KIND_FIELD_NUMBER, TaskKind.TASK_KIND_READ.getNumber());
        output.flush();
        byte[] repeated = scalar.toByteArray();
        class RepeatedScalarStream extends InputStream {
            private int bytesRead;
            @Override
            public int read() {
                return Byte.toUnsignedInt(repeated[bytesRead++ % repeated.length]);
            }

            @Override
            public int read(byte[] bytes, int offset, int length) {
                for (int index = 0; index < length; index++) {
                    bytes[offset + index] = (byte) read();
                }
                return length;
            }
        }
        var trailing = new RepeatedScalarStream();
        try (var resource = buildResource(); var body = new SequenceInputStream(
                new ByteArrayInputStream(prefix.toByteArray()), trailing);
                var response = resource.submitTask(body, null).toCompletableFuture().join()) {
            assertThat(response.getStatus()).isEqualTo(413);
            var ack = TaskSubmitAck.parseFrom((byte[]) response.getEntity());
            assertThat(ack.getRejected().getReason()).contains("HTTP limit");
            assertThat(prefix.size() + trailing.bytesRead).isEqualTo(limit + 1);
        }
    }

    private static TaskResource buildResource() {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport = TestExchangeTransport.INSTANCE;
        return new TaskResource(
                taskManager, exchangeTransport,
                new ExchangeSourceRegistry(), "localhost", 8816, null);
    }
}
