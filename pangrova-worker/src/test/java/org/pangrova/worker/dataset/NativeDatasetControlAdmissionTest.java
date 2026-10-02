package org.pangrova.worker.dataset;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeDatasetControlAdmissionTest {
    @Test
    void admitsExactlyEightCommandsAndIdempotentReleaseRestoresOneSlot() {
        var admission = new NativeDatasetControlAdmission();
        var leases = new ArrayList<NativeDatasetControlAdmission.Lease>();
        try {
            for (int i = 0; i < NativeDatasetControlAdmission.MAX_CONCURRENT_COMMANDS; i++) {
                leases.add(admission.acquire());
            }
            assertThatThrownBy(admission::acquire).isInstanceOf(ControlAdmissionRejectedException.class);
            leases.get(0).close();
            leases.get(0).close();
            try (var replacement = admission.acquire()) {
                assertThatThrownBy(admission::acquire).isInstanceOf(ControlAdmissionRejectedException.class);
            }
        } finally {
            leases.forEach(NativeDatasetControlAdmission.Lease::close);
            admission.close();
        }
        assertThatThrownBy(admission::acquire).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    void shutdownDrainsAnAdmittedCommandAndRejectsAllNewCommands() throws Exception {
        var admission = new NativeDatasetControlAdmission();
        var lease = admission.acquire();
        var executor = ManagedExecutorFactory.newSingleThreadExecutor("metadata-control-drain-test");
        var closeStarted = new CountDownLatch(1);
        try {
            var shutdown = executor.submit(() -> {
                closeStarted.countDown();
                admission.close();
            });
            assertThat(closeStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(shutdown.isDone()).isFalse();
            lease.close();
            shutdown.get(30, TimeUnit.SECONDS);
            assertThatThrownBy(admission::acquire).hasMessageContaining("closed");
            admission.close();
        } finally {
            lease.close();
            executor.shutdownNow();
            admission.close();
        }
    }
}
