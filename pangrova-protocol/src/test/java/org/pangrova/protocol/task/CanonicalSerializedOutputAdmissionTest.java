package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production admission authority; no execution or Native runtime is simulated. */
class CanonicalSerializedOutputAdmissionTest {
    @Test
    void directResultRequiresPositiveSerializedBufferGrant() {
        var request = request();
        request.getExecutionOptionsBuilder().setDirectResultOutput(true);
        request.getResourceGrantBuilder().setShuffleBufferBytes(0);
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request.build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive serialized-buffer grant");
        request.getResourceGrantBuilder().setShuffleBufferBytes(1);
        assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(request.build())).doesNotThrowAnyException();
    }

    @Test
    void noSerializedOutputDoesNotInventBufferGrant() {
        var request = request();
        request.getResourceGrantBuilder().setShuffleBufferBytes(0);
        assertThatCode(() -> CanonicalTaskRequests.validateForAdmission(request.build())).doesNotThrowAnyException();
    }

    private static TaskRequest.Builder request() {
        return CanonicalTaskRequestTestFixture.builder("output-task", "output-query", TaskKind.TASK_KIND_READ,
                new byte[] {1});
    }
}
