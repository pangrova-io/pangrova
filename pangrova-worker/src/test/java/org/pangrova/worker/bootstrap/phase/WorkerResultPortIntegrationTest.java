package org.pangrova.worker.bootstrap.phase;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerTaskOwnership;
import org.pangrova.worker.result.NativeResultTransport;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real packaged JNI/bootstrap transport ownership; not a Coordinator registration or process E2E proof. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerResultPortIntegrationTest {
    @Test
    void configuredPortBindsActualServerAndReadsCanonicalOutputToEof() throws Exception {
        int selected;
        try (var reservation = new ServerSocket(0)) { selected = reservation.getLocalPort(); }
        assertActualOutput(selected);
    }

    @Test
    void zeroPreservesActualOsPortAssignmentAndReadsCanonicalOutputToEof() throws Exception {
        assertActualOutput(0);
    }

    @Test
    void occupiedConfiguredPortFailsAndPartialTransportOwnersDrainBeforeRetry() throws Exception {
        try (var runtime = new ActualNativeTaskFixture(); var occupied = new ServerSocket(0);
             var ownership = ownership()) {
            runtime.stopTransports();
            assertThatThrownBy(() -> WorkerTaskBootstrap.createTransportRefs(
                    runtime.executor(), config(occupied.getLocalPort()), ownership))
                    .isInstanceOf(RuntimeException.class);
            assertThat(ownership.isDrained()).isFalse();
            ownership.close();
            assertThat(ownership.isDrained()).isTrue();
            try (var retry = ownership()) {
                var transports = WorkerTaskBootstrap.createTransportRefs(runtime.executor(), config(0), retry);
                assertThat(((NativeResultTransport) transports.resultTransport()).boundPort()).isBetween(1, 65_535);
            }
        }
    }

    private static void assertActualOutput(int preferredPort) throws Exception {
        try (var runtime = new ActualNativeTaskFixture(); var ownership = ownership()) {
            runtime.stopTransports();
            var transports = WorkerTaskBootstrap.createTransportRefs(runtime.executor(), config(preferredPort), ownership);
            var results = (NativeResultTransport) transports.resultTransport();
            int actualPort = results.boundPort();
            assertThat(actualPort).isBetween(1, 65_535);
            if (preferredPort != 0) assertThat(actualPort).isEqualTo(preferredPort);
            var request = runtime.valuesRequest(true);
            long handle = runtime.createStarted(request);
            var rows = new ArrayList<Long>();
            try (var allocator = new RootAllocator(ActualNativeTaskFixture.TASK_MEMORY_BYTES);
                 var client = FlightClient.builder(allocator, Location.forGrpcInsecure("127.0.0.1", actualPort)).build();
                 var stream = client.getStream(new Ticket(runtime.resultTicket(request).getBytes(StandardCharsets.UTF_8)))) {
                while (stream.next()) {
                    assertThat(stream.getRoot().getFieldVectors()).hasSize(1);
                    for (int row = 0; row < stream.getRoot().getRowCount(); row++) {
                        rows.add((Long) stream.getRoot().getVector(0).getObject(row));
                    }
                }
            }
            assertThat(rows).containsExactly(42L);
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            ownership.close();
            assertThat(ownership.isDrained()).isTrue();
            assertThat(results.boundPort()).isEqualTo(-1);
        }
    }

    private static WorkerTaskOwnership ownership() {
        return new WorkerTaskOwnership(WorkerBootstrapConfig.ResourceConfig.defaults().shuffleChannelLimits());
    }

    private static WorkerBootstrapConfig config(int port) {
        var defaults = WorkerBootstrapConfig.withDefaults("actual-result-port-worker");
        return new WorkerBootstrapConfig(defaults.workerId(), defaults.httpPort(), port, defaults.coordinatorUrl(),
                defaults.heartbeatIntervalMs(), defaults.observabilityEnabled(), defaults.observabilityBootstrapConfig(),
                defaults.resourceConfig(), defaults.resultRetentionSeconds(), defaults.cdcEnabled(),
                defaults.registrationRetryConfig(), defaults.httpHost(), defaults.internalAuthSecret(), defaults.shuffleMode(),
                defaults.cacheConfig(), "127.0.0.1", null,
                defaults.nativeFunctionConfig(), defaults.transportSecurityConfig());
    }
}
