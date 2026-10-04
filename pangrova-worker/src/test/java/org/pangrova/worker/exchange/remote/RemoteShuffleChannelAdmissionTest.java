package org.pangrova.worker.exchange.remote;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ErrorClass;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual channel-credit primitive tests, not RPC or task execution substitutes. */
class RemoteShuffleChannelAdmissionTest {
    @Test void rejectsInvalidLimits() {
        for (var values : new int[][]{{0, 1}, {1, 0}, {1, 2}}) {
            assertThatThrownBy(() -> new RemoteShuffleChannelAdmission.Limits(values[0], values[1]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void creatingCreditIsOccupiedUntilConstructionActuallyExits() {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(1, 1));
        var first = admission.reserve(endpoint(9400));
        assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(1);
        assertThatThrownBy(first::releaseTerminated).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> admission.reserve(endpoint(9401))).isInstanceOf(RemoteShuffleAdmissionException.class)
                .satisfies(failure -> assertThat(((RemoteShuffleAdmissionException) failure).scope()).isEqualTo("TOTAL"));
        first.constructionExited();
        first.releaseTerminated();
        first.releaseTerminated();
        assertThat(admission.snapshot().ownedAndCreatingChannels()).isZero();
        assertThat(admission.snapshot().endpointCount()).isZero();
    }

    @Test void endpointFailureRetainsExactClassificationAndExistingCredit() {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(2, 1));
        var first = admission.reserve(endpoint(9400));
        assertThatThrownBy(() -> admission.reserve(endpoint(9400))).isInstanceOf(RemoteShuffleAdmissionException.class)
                .satisfies(failure -> {
                    var typed = (RemoteShuffleAdmissionException) failure;
                    assertThat(typed.scope()).isEqualTo("ENDPOINT");
                    assertThat(typed.occupied()).isEqualTo(1);
                    assertThat(typed.limit()).isEqualTo(1);
                    assertThat(typed.executionError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_ADMISSION);
                    assertThat(typed.executionError().getCode()).isEqualTo("SHUFFLE_MANAGED_CHANNEL_LIMIT");
                    assertThat(typed.executionError().getRetryable()).isTrue();
                    assertThat(typed.executionError().getFailedComponent()).isEqualTo("remote-shuffle-channel-admission");
                });
        assertThat(admission.snapshot().channelsByEndpoint()).containsEntry(endpoint(9400), 1);
        first.constructionExited();
        first.releaseTerminated();
        admission.requireDrained();
    }

    @Test void concurrentCreationReservationsCannotOversubscribeTotalOrEndpoint() throws Exception {
        var admission = new RemoteShuffleChannelAdmission(new RemoteShuffleChannelAdmission.Limits(8, 8));
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(16, work -> new Thread(work, "channel-credit-boundary"));
        var futures = new ArrayList<Future<RemoteShuffleChannelAdmission.Reservation>>();
        try {
            for (int index = 0; index < 16; index++) futures.add(executor.submit(() -> {
                start.await();
                try { return admission.reserve(endpoint(9400)); }
                catch (RemoteShuffleAdmissionException rejected) { return null; }
            }));
            start.countDown();
            var admitted = new ArrayList<RemoteShuffleChannelAdmission.Reservation>();
            for (var future : futures) {
                var reservation = future.get(5, TimeUnit.SECONDS);
                if (reservation != null) admitted.add(reservation);
            }
            assertThat(admitted).hasSize(8);
            assertThat(admission.snapshot().ownedAndCreatingChannels()).isEqualTo(8);
            assertThat(admission.snapshot().channelsByEndpoint()).containsEntry(endpoint(9400), 8);
            for (var reservation : admitted) {
                reservation.constructionExited();
                reservation.releaseTerminated();
            }
            admission.requireDrained();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static RemoteShuffleChannelAdmission.Endpoint endpoint(int port) {
        return new RemoteShuffleChannelAdmission.Endpoint("localhost", port, "");
    }
}
