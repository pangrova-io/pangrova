package org.pangrova.connector.iceberg;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class IcebergCommitExecutorTest {

    @Test
    void saturatedPoolRejectsCommitWithoutExecutingItOnTheCaller() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.AbortPolicy());
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            pool.execute(() -> {
                running.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();
            pool.execute(() -> {});

            AtomicBoolean commitRan = new AtomicBoolean();
            IcebergCommitExecutor commits = new IcebergCommitExecutor(2_000, pool);
            Throwable failure = catchThrowable(() -> commits.execute(
                    () -> commitRan.set(true), "lake.orders"));
            assertThat(failure)
                    .isInstanceOf(ConnectorException.class)
                    .hasMessageContaining("admission rejected before execution");
            assertThat(((ConnectorException) failure).getErrorCode())
                    .isEqualTo(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR);
            assertThat(commitRan).isFalse();
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }
}
