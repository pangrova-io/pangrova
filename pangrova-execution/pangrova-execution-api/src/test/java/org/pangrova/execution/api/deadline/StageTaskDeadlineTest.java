package org.pangrova.execution.api.deadline;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageTaskDeadlineTest {
    @Test
    void bindsDurationToTheActualPreparationClock() {
        Instant preparedAt = Instant.parse("2026-01-01T00:00:00Z");
        var deadline = StageTaskDeadline.after(Duration.ofMillis(1), preparedAt);
        assertThat(deadline.preparedAt()).isEqualTo(preparedAt);
        assertThat(deadline.epochMillis()).isEqualTo(preparedAt.toEpochMilli() + 1);
    }

    @Test
    void refusesExpiredOrNonPositiveDispatchWindows() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertThatThrownBy(() -> new StageTaskDeadline(now.toEpochMilli(), now))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> StageTaskDeadline.after(Duration.ZERO, now))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StageTaskDeadline.after(Duration.ofMillis(-1), now))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StageTaskDeadline.after(Duration.ofNanos(1), now))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void refusesOverflowInsteadOfWrappingADeadline() {
        assertThatThrownBy(() -> StageTaskDeadline.after(Duration.ofMillis(1),
                Instant.ofEpochMilli(Long.MAX_VALUE))).isInstanceOf(ArithmeticException.class);
    }
}
