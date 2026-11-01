package org.pangrova.coordinator.bootstrap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.result.CoordinatorDataProducer;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.protocol.transport.TransportServerConfig;
import org.pangrova.protocol.transport.DataTicket;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.pangrova.worker.heartbeat.SpillDiskMonitor;
import org.pangrova.worker.transport.TransportServer;

class TransportServerTlsOwnerIntegrationTest {
    @TempDir
    Path workspace;

    @Test
    void validatesActualClientCertificateIdentity() throws Exception {
        try (var authority = new TestTlsAuthority(workspace);
             var foreign = new TestTlsAuthority(workspace);
             var allocator = new RootAllocator(4 * 1024 * 1024);
             var control = new ActualControlOwner()) {
            var identity = authority.issue("server", "localhost", false);
            var clientIdentity = authority.issue("client", "localhost", false);
            var foreignIdentity = foreign.issue("foreign", "localhost", false);
            var configuration = new TransportServerConfig(0, identity.certificateChain().toString(),
                    identity.privateKey().toString(), authority.root().toString(), true);
            var server = new TransportServer(control.producer(), configuration);
            try {
                server.start();
                check(server.isRunning() && server.getPort() > 0, "actual TLS server bound");
                var trusted = client(allocator, server.getPort(), authority.root(), clientIdentity);
                try {
                    var descriptor = FlightDescriptor.command(new byte[] {1, 2, 3});
                    check(trusted.getInfo(descriptor).getDescriptor().equals(descriptor), "trusted real mTLS RPC");
                } finally {
                    trusted.close();
                }
                rejectClient(allocator, server.getPort(), authority.root(), null);
                rejectClient(allocator, server.getPort(), authority.root(), foreignIdentity);
            } finally {
                server.stop();
            }
            check(!server.isRunning(), "server stopped after actual connections");
            check(allocator.getAllocatedMemory() == 0, "real client allocator drained");
        }
    }

    @Test
    void bootstrapUsesConfiguredTlsAndAdvertisesItsActualProtocol() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try (var authority = new TestTlsAuthority(workspace);
             var allocator = new RootAllocator(4 * 1024 * 1024)) {
            var identity = authority.issue("bootstrap-server", "localhost", false);
            var clientIdentity = authority.issue("bootstrap-client", "localhost", false);
            var base = CoordinatorBootstrapConfig.withDefaults("tls-bootstrap", 0);
            var tls = new org.pangrova.coordinator.transport.ShuffleTlsConfig(
                    identity.certificateChain(), identity.privateKey(), authority.root(), "localhost");
            var configuration = new CoordinatorBootstrapConfig(base.core(), base.featureFlags(), base.query(),
                    new CoordinatorBootstrapConfig.TransportConfig(0, 1024 * 1024, "localhost", tls),
                    base.workerHealth(), base.metadataJdbcConfig(), base.securityConfig(),
                    base.observabilityBootstrapConfig());
            var started = ControlPlaneResultTransportBootstrap.start(configuration, null, scheduler);
            try {
                var completed = new java.util.concurrent.atomic.AtomicBoolean();
                var result = started.service().dispatchControlPlaneRows("tls-ack",
                        java.util.List.of(org.pangrova.spi.connector.data.value.TypedRow.of(java.util.Map.of(
                                "status", new org.pangrova.spi.connector.data.value.ColumnValue.StringValue("OK")))),
                        java.util.List.of("status"), () -> completed.set(true));
                check(completed.get() && result.resultEndpoint().protocol().equals("grpc+tls")
                        && result.resultEndpoint().port() == started.server().getPort(),
                        "configured bootstrap advertises the actual encrypted endpoint");
                var actualServer = (TransportServer) started.server();
                check(org.pangrova.worker.transport.TransportServerAllocatorFixture
                        .allocator(actualServer).getLimit() == 1024 * 1024,
                        "interceptor constructor honors the configured actual allocator limit");
                var trusted = client(allocator, started.server().getPort(), authority.root(), clientIdentity);
                try {
                    var descriptor = FlightDescriptor.command(new byte[] {4, 5});
                    check(trusted.getInfo(descriptor).getDescriptor().equals(descriptor),
                            "bootstrap path reaches the real authenticated Flight server");
                    var ticket = new Ticket(new DataTicket("tls-ack", "", 0,
                            DataTicket.TicketType.RESULT).serialize());
                    for (int request = 0; request < 2; request++) {
                        try (var stream = trusted.getStream(ticket)) {
                            check(stream.next() && stream.getRoot().getRowCount() == 1,
                                    "control acknowledgement is returned once");
                            check(!stream.next(), "control acknowledgement reaches EOF");
                        }
                    }
                } finally {
                    trusted.close();
                }
                rejectClient(allocator, started.server().getPort(), authority.root(), null);
            } finally {
                started.server().stop();
                check(started.buffer().contains("tls-ack"), "actual acknowledgement remains owned until close");
                started.buffer().close();
                check(started.buffer().bufferedBytes() == 0 && started.buffer().size() == 0,
                        "actual close retires acknowledgement bytes and expiry owner");
                check(started.allocator().getAllocatedMemory() == 0, "actual acknowledgement allocator drained");
                started.allocator().close();
            }
        } finally {
            scheduler.shutdownNow();
            check(scheduler.awaitTermination(10, TimeUnit.SECONDS), "actual bootstrap scheduler joined");
        }
    }

    @Test
    void rejectsIncompleteTlsWithoutSuccessfulRunningState() {
        try (var control = new ActualControlOwner()) {
            for (var invalid : new TransportServerConfig[] {
                    new TransportServerConfig(0, workspace.toString(), null),
                    new TransportServerConfig(0, workspace.toString(), workspace.toString(), null, true),
                    new TransportServerConfig(0, workspace.toString(), workspace.toString())}) {
                var broken = new TransportServer(control.producer(), invalid);
                boolean rejected = false;
                try {
                    broken.start();
                } catch (IllegalStateException expected) {
                    rejected = true;
                }
                check(rejected && !broken.isRunning(), "invalid TLS must fail without false running state");
                broken.stop();
            }
        }
    }

    @Test
    void retainsActualMemoryUntilReleaseAndAcceptsOnlySpillDirectories() throws Exception {
        try (var control = new ActualControlOwner()) {
            var owner = new TransportServer(control.producer(), new TransportServerConfig(0));
            var actualOwner = org.pangrova.worker.transport.TransportServerAllocatorFixture.allocator(owner);
            var actualBuffer = actualOwner.buffer(8192);
            try {
                owner.start();
                boolean retained = false;
                try {
                    owner.stop();
                } catch (IllegalStateException expected) {
                    retained = true;
                }
                check(retained && actualOwner.getAllocatedMemory() == 8192,
                        "failed actual drain retains allocation and rejects successful stop");
                boolean admissionSealed = false;
                try {
                    owner.start();
                } catch (IllegalStateException expected) {
                    admissionSealed = true;
                }
                check(admissionSealed && actualOwner.getAllocatedMemory() == 8192,
                        "failed close cannot reopen admission or discard its live budget");
            } finally {
                actualBuffer.close();
                owner.stop();
            }
            check(actualOwner.getAllocatedMemory() == 0, "actual release precedes successful retry");
            var disk = new SpillDiskMonitor(workspace.toString());
            check(disk.isConfigured() && disk.getAvailableBytes() > 0, "actual directory filesystem space");
            Path regular = Files.createTempFile(workspace, "not-spill-directory-", ".txt");
            try {
                var fileMonitor = new SpillDiskMonitor(regular.toString());
                check(!fileMonitor.isConfigured() && fileMonitor.getAvailableBytes() == 0,
                        "regular file rejects spill directory");
            } finally {
                Files.delete(regular);
            }
            check(new SpillDiskMonitor(null).getAvailableBytes() == 0, "unconfigured directory");
            check(new SpillDiskMonitor(workspace.resolve("absent").toString()).getAvailableBytes() == 0,
                    "absent directory");
        }
    }

    private static FlightClient client(BufferAllocator allocator, int port, Path trust,
            TestTlsAuthority.Identity identity) throws Exception {
        try (var trusted = Files.newInputStream(trust);
             var certificate = identity == null ? null : Files.newInputStream(identity.certificateChain());
             var key = identity == null ? null : Files.newInputStream(identity.privateKey())) {
            var builder = FlightClient.builder(allocator, Location.forGrpcTls("localhost", port))
                    .trustedCertificates(trusted);
            if (identity != null) {
                builder.clientCertificate(certificate, key);
            }
            return builder.build();
        }
    }

    private static void rejectClient(BufferAllocator allocator, int port, Path trust,
            TestTlsAuthority.Identity identity) throws Exception {
        boolean rejected = false;
        var client = client(allocator, port, trust, identity);
        try {
            try {
                client.getInfo(FlightDescriptor.command(new byte[] {1}));
            } catch (org.apache.arrow.flight.FlightRuntimeException expected) {
                rejected = true;
            }
        } finally {
            client.close();
        }
        check(rejected, "actual TLS handshake rejected unauthorized identity");
    }

    private static void check(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static final class ActualControlOwner implements AutoCloseable {
        private final java.util.concurrent.ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        private final CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(scheduler, 60_000, 4, 1024);
        private final CoordinatorDataProducer producer = new CoordinatorDataProducer(buffer);

        CoordinatorDataProducer producer() {
            return producer;
        }

        @Override
        public void close() {
            buffer.close();
            scheduler.shutdownNow();
            try {
                check(scheduler.awaitTermination(10, TimeUnit.SECONDS), "owned scheduler joined");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Owned scheduler join interrupted", interrupted);
            }
        }
    }
}
