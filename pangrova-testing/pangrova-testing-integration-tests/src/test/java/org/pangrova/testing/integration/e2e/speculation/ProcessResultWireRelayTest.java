package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrency.ManagedExecutorFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real TCP regression for the formerly shared forward/reverse blocking-copy pool. */
class ProcessResultWireRelayTest {
    private static final int CONNECTIONS = 31;
    private static final int GREETING = 0x5a;

    @Test
    void allAdmittedConnectionsReceiveReverseBytesAfterForwardPoolIsFull() throws Exception {
        CountDownLatch targetEntered = new CountDownLatch(CONNECTIONS);
        CountDownLatch releaseTargets = new CountDownLatch(1);
        List<Socket> clients = new ArrayList<>();
        List<Socket> backendPeers = new CopyOnWriteArrayList<>();
        ExecutorService backendOwner = ManagedExecutorFactory.newSingleThreadExecutor("result-relay-test-backend");
        try (ServerSocket backend = new ServerSocket(0, CONNECTIONS, InetAddress.getLoopbackAddress());
             ProcessResultWireRelay relay = new ProcessResultWireRelay(() -> {
                 targetEntered.countDown();
                 try {
                     if (!releaseTargets.await(30, TimeUnit.SECONDS)) {
                         throw new IllegalStateException("Not all forward owners reached the target barrier");
                     }
                 } catch (InterruptedException interrupted) {
                     Thread.currentThread().interrupt();
                     throw new IllegalStateException("Target barrier interrupted", interrupted);
                 }
                 return new InetSocketAddress(InetAddress.getLoopbackAddress(), backend.getLocalPort());
             })) {
            Future<Integer> accepted = backendOwner.submit(() -> {
                for (int number = 0; number < CONNECTIONS; number++) {
                    Socket peer = backend.accept();
                    backendPeers.add(peer);
                    peer.getOutputStream().write(GREETING);
                    peer.getOutputStream().flush();
                }
                return backendPeers.size();
            });
            try {
                for (int number = 0; number < CONNECTIONS; number++) {
                    Socket client = new Socket(InetAddress.getLoopbackAddress(), relay.port());
                    client.setSoTimeout((int) Duration.ofSeconds(10).toMillis());
                    clients.add(client);
                }
                assertThat(targetEntered.await(30, TimeUnit.SECONDS)).isTrue();
                releaseTargets.countDown();

                for (Socket client : clients) {
                    assertThat(client.getInputStream().read()).isEqualTo(GREETING);
                }
                assertThat(accepted.get(10, TimeUnit.SECONDS)).isEqualTo(CONNECTIONS);
                assertThat(relay.forwardingFailures()).isEmpty();
            } finally {
                releaseTargets.countDown();
                try { relay.close(); }
                finally { closeSockets(clients, backendPeers); }
            }
        } finally {
            backendOwner.shutdownNow();
            assertThat(backendOwner.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void closeSockets(List<Socket> clients, List<Socket> backendPeers) throws IOException {
        IOException failure = null;
        List<Socket> sockets = new ArrayList<>(clients);
        sockets.addAll(backendPeers);
        for (Socket socket : sockets) {
            try { socket.close(); }
            catch (IOException cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) throw failure;
    }
}
