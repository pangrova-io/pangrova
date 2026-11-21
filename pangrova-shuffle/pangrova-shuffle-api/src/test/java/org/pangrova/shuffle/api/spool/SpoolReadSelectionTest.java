package org.pangrova.shuffle.api.spool;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SpoolReadSelectionTest {
    @Test void selectionIsImmutableOrderedAndKeepsLongAttempts() {
        var input = new HashMap<Integer, Long>();
        input.put(9, Long.MAX_VALUE);
        input.put(2, 0L);
        var selected = new SpoolReadSelection("q", 1, 2, input);
        input.put(2, 99L);
        assertThat(selected.producerAttempts().keySet()).containsExactly(2, 9);
        assertThat(selected.producerAttempts()).containsEntry(2, 0L).containsEntry(9, Long.MAX_VALUE);
        assertThatThrownBy(() -> selected.producerAttempts().put(1, 0L)).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void invalidIdentitiesAreNotNormalizedIntoAnotherAttempt() {
        assertThatThrownBy(() -> new SpoolReadSelection("q", -1, 0, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolReadSelection("q", 0, -1, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolReadSelection("q", 0, 0, Map.of(-1, 0L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolReadSelection("q", 0, 0, Map.of(0, -1L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolReadSelection("", 0, 0, Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SpoolReadSelection(null, 0, 0, Map.of())).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SpoolReadSelection("q", 0, 0, null)).isInstanceOf(NullPointerException.class);
        var invalid = new HashMap<Integer, Long>();
        invalid.put(0, null);
        assertThatThrownBy(() -> new SpoolReadSelection("q", 0, 0, invalid)).isInstanceOf(IllegalArgumentException.class);
    }
}
