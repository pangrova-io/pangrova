package org.pangrova.testing.integration.e2e.cancellation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.testing.testkit.E2ETest;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL/Native Worker and delayed task HTTP. Run in its own JVM: the fence is permanent. */
@E2ETest
@Tag("native-required")
@Tag("native-runtime-owner")
class WorkerMembershipAdmissionE2ETest extends TaskCancellationTestBase {
    @Test
    void membershipFenceRejectsPreviouslyIssuedTaskAndPreservesCleanupReceipt() throws Exception {
        try (var scenario = scenario()) {
            actualWorkerTaskManager().fenceAdmission();
            actualWorkerTaskManager().fenceAdmission();
            var rejected = replay(scenario.submission.body(), scenario.submission.headers());
            assertThat(rejected.statusCode()).isEqualTo(503);
            assertThat(TaskSubmitAck.parseFrom(rejected.body()).hasRejected()).isTrue();
            assertThat(cancelQuery(scenario.queryId()).statusCode()).isEqualTo(202);
            var receipt = receipt(scenario.transport.awaitDrainResponse(), scenario.submission.request());
            assertThat(receipt.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            scenario.transport.releasePost();
            assertThat(scenario.transport.awaitPostResponse().statusCode()).isIn(409, 503);
            assertThat(pollUntilTerminal(scenario.queryId())).contains("\"CANCELLED\"");
        }
    }
}
