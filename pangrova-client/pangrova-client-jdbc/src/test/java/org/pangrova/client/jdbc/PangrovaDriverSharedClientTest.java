package org.pangrova.client.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;

import java.net.http.HttpClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Verifies R10.5: PangrovaDriver shares a single HttpClient across JDBC connections.
 */
class PangrovaDriverSharedClientTest {

    @AfterEach
    void tearDown() {
        PangrovaDriver.shutdownSharedClient();
    }

    @Test
    void acceptsURL_validPangrovaUrl_returnsTrue() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.acceptsURL("jdbc:pangrova://localhost:8080")).isTrue();
        assertThat(driver.acceptsURL("jdbc:pangrova://host:9090/catalog")).isTrue();
    }

    @Test
    void acceptsURL_nonPangrovaUrl_returnsFalse() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.acceptsURL("jdbc:mysql://localhost:3306")).isFalse();
        assertThat(driver.acceptsURL(null)).isFalse();
    }

    @Test
    void shutdownSharedClient_isIdempotent() {
        // Should not throw even when called multiple times without creating a client
        PangrovaDriver.shutdownSharedClient();
        PangrovaDriver.shutdownSharedClient();
    }

    @Test
    void majorVersion_isZero() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.getMajorVersion()).isEqualTo(0);
        assertThat(driver.getMinorVersion()).isEqualTo(1);
    }

    @Test
    void jdbcCompliant_returnsFalse() {
        PangrovaDriver driver = new PangrovaDriver();
        assertThat(driver.jdbcCompliant()).isFalse();
    }

    @Test
    void concurrentFirstConnectionsShareOneActualClientAndExecutor() throws Exception {
        var owner = new JdbcSharedHttpOwner();
        var callers = ManagedExecutorFactory.newFixedThreadPool("actual-shared-http-initialization", 8);
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        var clients = new java.util.ArrayList<java.util.concurrent.Future<HttpClient>>();
        try {
            for (int i = 0; i < 8; i++) clients.add(callers.submit(() -> {
                ready.countDown();
                if (!start.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("Actual concurrent callers were not released");
                return owner.get(2, true);
            }));
            assertThat(ready.await(2, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var first = clients.get(0).get(5, TimeUnit.SECONDS);
            for (var client : clients) assertThat(client.get(5, TimeUnit.SECONDS)).isSameAs(first);
            var executor = (java.util.concurrent.ThreadPoolExecutor) first.executor().orElseThrow();
            assertThat(executor.getCorePoolSize()).isEqualTo(2);
        } finally {
            start.countDown();
            try { ExecutorDrain.shutdownAndAwait(callers, 5_000); }
            finally { owner.close(); }
        }
    }

    @Test
    void sharedExecutorTimeoutRetainsOwnerAndSealsAdmissionUntilActualRetry() throws Exception {
        var owner = new JdbcSharedHttpOwner();
        var client = owner.get(2, true);
        var executor = (ExecutorService) client.executor().orElseThrow();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        holdActualExecutor(client, entered, release, exited);
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not actually terminated");
            assertThat(executor.isTerminated()).isFalse();
            assertThat(exited.getCount()).isEqualTo(1);
            assertThatThrownBy(() -> owner.get(2, true)).hasMessageContaining("admission is sealed");
        } finally {
            release.countDown();
            owner.close();
        }
        assertThat(executor.isTerminated()).isTrue();
        assertThat(exited.getCount()).isZero();
        try { assertThat(owner.get(2, true)).isNotSameAs(client); }
        finally { owner.close(); }
    }

    @Test
    void interruptedSharedClosePreservesInterruptAndRealOwnerForRetry() throws Exception {
        var owner = new JdbcSharedHttpOwner();
        var client = owner.get(2, true);
        var executor = (ExecutorService) client.executor().orElseThrow();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        holdActualExecutor(client, entered, release, exited);
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            assertThatThrownBy(owner::close).hasMessageContaining("Interrupted awaiting actual executor termination");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(executor.isTerminated()).isFalse();
            assertThatThrownBy(() -> owner.get(2, true)).hasMessageContaining("admission is sealed");
        } finally {
            Thread.interrupted();
            release.countDown();
            owner.close();
        }
        assertThat(executor.isTerminated()).isTrue();
        assertThat(exited.getCount()).isZero();
    }

    @Test
    void actualSharedJoinDoesNotHoldTheAdmissionMonitor() throws Exception {
        var owner = new JdbcSharedHttpOwner();
        var client = owner.get(2, true);
        var executor = (ExecutorService) client.executor().orElseThrow();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var closer = ManagedExecutorFactory.newSingleThreadExecutor("actual-shared-http-close");
        var probe = ManagedExecutorFactory.newSingleThreadExecutor("actual-shared-http-admission");
        holdActualExecutor(client, entered, release, exited);
        var close = closer.submit(owner::close);
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            await().atMost(1, TimeUnit.SECONDS).until(executor::isShutdown);
            probe.submit(() -> assertThatThrownBy(() -> owner.get(2, true))
                    .hasMessageContaining("admission is sealed")).get(1, TimeUnit.SECONDS);
            assertThat(close.isDone()).isFalse();
        } finally {
            release.countDown();
            finishClosing(close, closer, probe, owner);
        }
        assertThat(executor.isTerminated()).isTrue();
        assertThat(exited.getCount()).isZero();
    }

    private static void finishClosing(java.util.concurrent.Future<?> close, ExecutorService closer,
                                      ExecutorService probe, JdbcSharedHttpOwner owner) throws Exception {
        Exception failure = null;
        try { close.get(6, TimeUnit.SECONDS); }
        catch (Exception cleanupFailure) { failure = cleanupFailure; }
        for (var executor : java.util.List.of(closer, probe)) {
            try { ExecutorDrain.shutdownAndAwait(executor, 5_000); }
            catch (RuntimeException cleanupFailure) {
                if (failure == null) failure = cleanupFailure;
                else failure.addSuppressed(cleanupFailure);
            }
        }
        try { owner.close(); }
        catch (RuntimeException cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else failure.addSuppressed(cleanupFailure);
        }
        if (failure != null) throw failure;
    }

    private static void holdActualExecutor(HttpClient client, CountDownLatch entered,
                                          CountDownLatch release, CountDownLatch exited) {
        var executor = (ExecutorService) client.executor().orElseThrow();
        executor.execute(() -> ExecutorDrain.runOwned(executor, () -> {
            entered.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("Actual executor was not released");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            } finally { exited.countDown(); }
        }));
    }
}
