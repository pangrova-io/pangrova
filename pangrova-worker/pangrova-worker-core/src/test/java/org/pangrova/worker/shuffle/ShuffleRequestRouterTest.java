package org.pangrova.worker.shuffle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ShuffleRequestRouter}.
 *
 * <p>Validates ticket codec (encode/decode round-trip), handle registration
 * lifecycle, and error handling for malformed tickets.</p>
 */
class ShuffleRequestRouterTest {

    private ShuffleRequestRouter router;

    @BeforeEach
    void setUp() {
        router = new ShuffleRequestRouter();
    }

    // ── Ticket codec ────────────────────────────────────────────────

    @Test
    void encodeTicket_producesNewlineDelimitedFormat() {
        String ticket = ShuffleRequestRouter.encodeTicket(42, "query-abc", 3, 7);
        assertThat(ticket).isEqualTo("42\nquery-abc\n3\n7");
    }

    @Test
    void decodeTicket_roundTrip() {
        String ticket = ShuffleRequestRouter.encodeTicket(100, "q-1", 5, 12);
        ShuffleRequestRouter.TicketComponents components =
                ShuffleRequestRouter.decodeTicket(ticket);

        assertThat(components.executorHandle()).isEqualTo(100);
        assertThat(components.queryId()).isEqualTo("q-1");
        assertThat(components.stageId()).isEqualTo(5);
        assertThat(components.partitionId()).isEqualTo(12);
    }

    @Test
    void decodeTicket_handlesLargeHandle() {
        long largeHandle = Long.MAX_VALUE;
        String ticket = ShuffleRequestRouter.encodeTicket(largeHandle, "q", 0, 0);
        ShuffleRequestRouter.TicketComponents components =
                ShuffleRequestRouter.decodeTicket(ticket);

        assertThat(components.executorHandle()).isEqualTo(largeHandle);
    }

    @Test
    void decodeTicket_rejectsNullTicket() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void decodeTicket_rejectsEmptyTicket() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void decodeTicket_rejectsTooFewFields() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket("42\nquery"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected 4");
    }

    @Test
    void decodeTicket_rejectsTooManyFields() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket("42\nq\n1\n2\nextra"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected 4");
    }

    @Test
    void decodeTicket_rejectsNonNumericHandle() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket("abc\nq\n1\n2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-numeric");
    }

    @Test
    void decodeTicket_rejectsNonNumericStageId() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket("42\nq\nfoo\n2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-numeric");
    }

    @Test
    void decodeTicket_rejectsNonNumericPartitionId() {
        assertThatThrownBy(() -> ShuffleRequestRouter.decodeTicket("42\nq\n1\nbar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-numeric");
    }

    // ── Handle registration ─────────────────────────────────────────

    @Test
    void registerHandle_tracksRegistration() {
        router.registerHandle(1, "q1", 0, 4);

        assertThat(router.activeHandleCount()).isEqualTo(1);
        ShuffleRequestRouter.HandleRegistration reg = router.getRegistration(1);
        assertThat(reg).isNotNull();
        assertThat(reg.executorHandle()).isEqualTo(1);
        assertThat(reg.queryId()).isEqualTo("q1");
        assertThat(reg.stageId()).isEqualTo(0);
        assertThat(reg.numPartitions()).isEqualTo(4);
        assertThat(reg.registeredAtMs()).isGreaterThan(0);
    }

    @Test
    void unregisterHandle_removesRegistration() {
        router.registerHandle(1, "q1", 0, 4);
        ShuffleRequestRouter.HandleRegistration removed = router.unregisterHandle(1);

        assertThat(removed).isNotNull();
        assertThat(removed.executorHandle()).isEqualTo(1);
        assertThat(router.activeHandleCount()).isEqualTo(0);
        assertThat(router.getRegistration(1)).isNull();
    }

    @Test
    void unregisterHandle_returnsNullForUnknown() {
        ShuffleRequestRouter.HandleRegistration removed = router.unregisterHandle(999);
        assertThat(removed).isNull();
    }

    @Test
    void multipleHandles_trackedIndependently() {
        router.registerHandle(1, "q1", 0, 4);
        router.registerHandle(2, "q1", 1, 8);
        router.registerHandle(3, "q2", 0, 2);

        assertThat(router.activeHandleCount()).isEqualTo(3);

        router.unregisterHandle(2);
        assertThat(router.activeHandleCount()).isEqualTo(2);
        assertThat(router.getRegistration(1)).isNotNull();
        assertThat(router.getRegistration(2)).isNull();
        assertThat(router.getRegistration(3)).isNotNull();
    }

    @Test
    void snapshot_returnsImmutableCopy() {
        router.registerHandle(1, "q1", 0, 4);
        var snap = router.snapshot();

        assertThat(snap).hasSize(1);
        // Modifying the router after snapshot should not affect the snapshot
        router.registerHandle(2, "q2", 1, 8);
        assertThat(snap).hasSize(1);
        assertThat(router.activeHandleCount()).isEqualTo(2);
    }

    @Test
    void emptyRouter_hasZeroHandles() {
        assertThat(router.activeHandleCount()).isEqualTo(0);
        assertThat(router.snapshot()).isEmpty();
        assertThat(router.getRegistration(1)).isNull();
    }
}
