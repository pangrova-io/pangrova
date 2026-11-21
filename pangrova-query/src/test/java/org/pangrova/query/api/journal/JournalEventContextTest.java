package org.pangrova.query.api.journal;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JournalEventContextTest {
    private static final UUID EVENT_ID = UUID.fromString("61a75305-9927-4726-9633-8452c7d5b10f");

    @Test
    void retainsExactIdentityOriginalTimeAndIssuingOwner() {
        var context = new JournalEventContext(EVENT_ID, 1234L, "issuing-coordinator");
        assertThat(context.eventId()).isEqualTo(EVENT_ID);
        assertThat(context.timestampMs()).isEqualTo(1234L);
        assertThat(context.coordinatorId()).isEqualTo("issuing-coordinator");
        assertThat(context).isEqualTo(new JournalEventContext(EVENT_ID, 1234L, "issuing-coordinator"));
    }

    @Test
    void permitsZeroTimeAndAnExplicitUnownedEvent() {
        var context = new JournalEventContext(EVENT_ID, 0L, null);
        assertThat(context.timestampMs()).isZero();
        assertThat(context.coordinatorId()).isNull();
    }

    @Test
    void rejectsMissingIdentityAndNegativeOriginalTime() {
        assertThatThrownBy(() -> new JournalEventContext(null, 1L, "coordinator"))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("eventId");
        assertThatThrownBy(() -> new JournalEventContext(EVENT_ID, -1L, "coordinator"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timestampMs");
    }

    @Test
    void newTransitionsReceiveDistinctIdentitiesWithoutChangingTheirOriginalTime() {
        var first = JournalEventContext.create(1234L, "coordinator");
        var second = JournalEventContext.create(1234L, "coordinator");
        assertThat(first.eventId()).isNotEqualTo(second.eventId());
        assertThat(first.timestampMs()).isEqualTo(second.timestampMs());
        assertThat(first.coordinatorId()).isEqualTo(second.coordinatorId());
    }
}
