package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TaskRequestFactory#buildDmlWriteRequest}, verifying
 * that DML write task requests are constructed correctly.
 */
class TaskRequestEpochStampingTest {

    @Test
    void dmlWriteRequest_hasCorrectTaskType() {
        var session = new CoordinatorSessionContext("q-2", Map.of(), null, "tenant-1");
        var limit = new ResourceLimitPayload(1024L, 4, 0L, "/tmp");
        var writeConfig = new WriteConfig("catalog.schema.table", "APPEND", List.of(), null, 0L);
        SubmitTaskRequest req = TaskRequestFactory.buildDmlWriteRequest(
                session, new byte[]{1, 2, 3}, limit, List.of(), List.of(), writeConfig);
        assertThat(req.taskType()).isEqualTo("WRITE");
    }

    @Test
    void dmlWriteRequest_carriesQueryId() {
        var session = new CoordinatorSessionContext("q-3", Map.of(), null, null);
        var limit = new ResourceLimitPayload(1024L, 4, 0L, "/tmp");
        var writeConfig = new WriteConfig("catalog.schema.table", "APPEND", List.of(), null, 0L);
        SubmitTaskRequest req = TaskRequestFactory.buildDmlWriteRequest(
                session, new byte[]{1}, limit, List.of(), List.of(), writeConfig);
        assertThat(req.queryId()).isEqualTo("q-3");
    }

    @Test
    void dmlWriteRequest_carriesWriteConfig() {
        var session = new CoordinatorSessionContext("q-4", Map.of(), null, "tenant-1");
        var limit = new ResourceLimitPayload(1024L, 4, 0L, "/tmp");
        var writeConfig = new WriteConfig("catalog.schema.orders", "APPEND", List.of(), null, 0L);
        SubmitTaskRequest req = TaskRequestFactory.buildDmlWriteRequest(
                session, new byte[]{1, 2}, limit, List.of(), List.of(), writeConfig);
        assertThat(req.writeConfig()).isNotNull();
        assertThat(req.writeConfig().targetTable()).isEqualTo("catalog.schema.orders");
    }

    @Test
    void dmlWriteRequest_generatesUniqueTaskId() {
        var session = new CoordinatorSessionContext("q-5", Map.of(), null, null);
        var limit = new ResourceLimitPayload(1024L, 4, 0L, "/tmp");
        var writeConfig = new WriteConfig("catalog.schema.table", "APPEND", List.of(), null, 0L);
        SubmitTaskRequest req1 = TaskRequestFactory.buildDmlWriteRequest(
                session, new byte[]{1}, limit, List.of(), List.of(), writeConfig);
        SubmitTaskRequest req2 = TaskRequestFactory.buildDmlWriteRequest(
                session, new byte[]{1}, limit, List.of(), List.of(), writeConfig);
        assertThat(req1.taskId()).isNotEqualTo(req2.taskId());
    }
}
