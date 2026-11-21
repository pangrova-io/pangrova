package org.pangrova.worker.task;

import org.pangrova.worker.exception.ExecutionOwnerFenceException;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskControlOwnerFenceTest {
    @Test
    void controlAndTaskAdmissionShareOneExactOwnerFence() {
        try (var manager = TaskManager.forHttpWiringOnly()) {
            var first = identity("owner-a", 10);
            var second = identity("owner-b", 11);
            manager.verifyExecutionOwnerForAdmission(task(first));
            assertThatThrownBy(() -> manager.verifyExecutionOwnerForControl("execution-1", identity("owner-b", 10)))
                    .isInstanceOf(ExecutionOwnerFenceException.class);
            manager.verifyExecutionOwnerForControl("execution-1", second);
            assertThatThrownBy(() -> manager.verifyExecutionOwnerForAdmission(task(first)))
                    .isInstanceOf(ExecutionOwnerFenceException.class);
            assertThatCode(() -> manager.verifyExecutionOwnerForAdmission(task(second))).doesNotThrowAnyException();
        }
    }

    private static ExecutionOwnerIdentity identity(String owner, long token) {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId(owner).setOwnerIncarnation(owner + "-incarnation")
                .setFencingToken(token).build();
    }

    private static SubmitTaskRequest task(ExecutionOwnerIdentity owner) {
        return SubmitTaskRequest.from(TaskRequest.newBuilder().setAttempt(TaskAttemptId.newBuilder()
                .setQueryOrJobId("execution-1").setTaskId("task-1").setExecutionOwner(owner)).build());
    }
}
