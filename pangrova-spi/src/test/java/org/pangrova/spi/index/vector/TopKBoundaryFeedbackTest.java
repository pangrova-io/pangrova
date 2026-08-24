package org.pangrova.spi.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TopKBoundaryFeedback} (Task 165: top-k boundary feedback channel).
 */
class TopKBoundaryFeedbackTest {

    @Test
    void completeFeedback() {
        var feedback = new TopKBoundaryFeedback("w-1", "q-1", 10, 3.14, 10);
        assertEquals("w-1", feedback.workerId());
        assertEquals("q-1", feedback.queryId());
        assertEquals(10, feedback.topK());
        assertEquals(3.14, feedback.worstDistance(), 0.001);
        assertEquals(10, feedback.resultCount());
        assertTrue(feedback.isComplete());
    }

    @Test
    void incompleteFeedback() {
        var feedback = new TopKBoundaryFeedback("w-1", "q-1", 10, Double.MAX_VALUE, 3);
        assertFalse(feedback.isComplete());
    }

    @Test
    void rejectsNullWorkerId() {
        assertThrows(IllegalArgumentException.class,
                () -> new TopKBoundaryFeedback(null, "q-1", 10, 1.0, 10));
    }

    @Test
    void rejectsNullQueryId() {
        assertThrows(IllegalArgumentException.class,
                () -> new TopKBoundaryFeedback("w-1", null, 10, 1.0, 10));
    }

    @Test
    void rejectsNonPositiveTopK() {
        assertThrows(IllegalArgumentException.class,
                () -> new TopKBoundaryFeedback("w-1", "q-1", 0, 1.0, 0));
    }
}
