package org.pangrova.server.coordinator.http.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import jakarta.ws.rs.core.Response;

import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentConfig;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.protocol.rest.task.SplitRequest;
import org.pangrova.protocol.rest.task.SplitResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SplitResource} endpoint.
 */
class SplitResourceTest {

    /** Creates a SplitAssignmentManager with pull-mode enabled for testing. */
    private static SplitAssignmentManager pullModeManager() {
        return new SplitAssignmentManager(
                new SplitAssignmentConfig(60_000, (CacheAffinityAssigner) null, 0, true));
    }

    @Test
    void requestSplitReturnsSplitWhenAvailable() {
        SplitAssignmentManager manager = pullModeManager();
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "split-data-0", List.of("host-a"))));

        SplitResource resource = new SplitResource(manager);
        SplitRequest request = new SplitRequest("q1", 0, "w1", List.of("host-a"));

        try (Response response = resource.requestSplit(request)) {
            assertEquals(200, response.getStatus());
            SplitResponse body = (SplitResponse) response.getEntity();
            assertTrue(body.hasSplit());
            assertEquals("split-data-0", body.splitData());
            assertEquals(0, body.splitIndex());
            assertTrue(body.hasMore());
        }
    }

    @Test
    void requestSplitReturnsNoMoreSplitsWhenEmpty() {
        SplitAssignmentManager manager = pullModeManager();
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "split-data-0", List.of())));

        SplitResource resource = new SplitResource(manager);

        // Drain the queue
        resource.requestSplit(new SplitRequest("q1", 0, "w1", List.of()));

        // Now request again — should be empty
        try (Response response = resource.requestSplit(
                new SplitRequest("q1", 0, "w1", List.of()))) {
            assertEquals(200, response.getStatus());
            SplitResponse body = (SplitResponse) response.getEntity();
            assertFalse(body.hasSplit());
            assertFalse(body.hasMore());
        }
    }

    @Test
    void requestSplitReturnsNoMoreSplitsForUnknownStage() {
        SplitAssignmentManager manager = pullModeManager();
        SplitResource resource = new SplitResource(manager);

        try (Response response = resource.requestSplit(
                new SplitRequest("unknown", 99, "w1", List.of()))) {
            assertEquals(200, response.getStatus());
            SplitResponse body = (SplitResponse) response.getEntity();
            assertFalse(body.hasSplit());
        }
    }

    @Test
    void requestSplitWithNullBodyReturnsBadRequest() {
        SplitAssignmentManager manager = pullModeManager();
        SplitResource resource = new SplitResource(manager);

        try (Response response = resource.requestSplit(null)) {
            assertEquals(400, response.getStatus());
        }
    }

    @Test
    void completeSplitMarksCompleted() {
        SplitAssignmentManager manager = pullModeManager();
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of())));

        SplitResource resource = new SplitResource(manager);

        // Request and complete the split
        resource.requestSplit(new SplitRequest("q1", 0, "w1", List.of()));
        try (Response response = resource.completeSplit(
                new SplitResource.SplitCompletionRequest("q1", 0, 0))) {
            assertEquals(200, response.getStatus());
        }

        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    @Test
    void completeSplitWithNullBodyReturnsBadRequest() {
        SplitAssignmentManager manager = pullModeManager();
        SplitResource resource = new SplitResource(manager);

        try (Response response = resource.completeSplit(null)) {
            assertEquals(400, response.getStatus());
        }
    }

    @Test
    void workerPullsMultipleSplitsUntilEmpty() {
        SplitAssignmentManager manager = pullModeManager();
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of()),
                new SplitAssignment(1, "s1", List.of()),
                new SplitAssignment(2, "s2", List.of())));

        SplitResource resource = new SplitResource(manager);

        int pulled = 0;
        while (true) {
            try (Response response = resource.requestSplit(
                    new SplitRequest("q1", 0, "w1", List.of()))) {
                SplitResponse body = (SplitResponse) response.getEntity();
                if (!body.hasSplit()) break;
                pulled++;
                resource.completeSplit(
                        new SplitResource.SplitCompletionRequest("q1", 0, body.splitIndex()));
            }
        }

        assertEquals(3, pulled);
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }
}
