package org.pangrova.spi.indexing.feature;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link FeatureValue}.
 */
class FeatureValueTest {

    private static final Instant NOW = Instant.parse("2026-06-01T12:00:00Z");

    private static FeatureValue sample() {
        return new FeatureValue(
                "user-42", "user_profile",
                Map.of("age", 30, "country", "US"), NOW);
    }

    // ── Field accessors ─────────────────────────────────────────────

    @Test
    void fieldAccessorsReturnConstructorValues() {
        var fv = sample();
        assertEquals("user-42", fv.entityId());
        assertEquals("user_profile", fv.featureGroup());
        assertEquals(Map.of("age", 30, "country", "US"), fv.values());
        assertEquals(NOW, fv.eventTime());
    }

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void rejectsNullEntityId() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue(null, "fg", Map.of("f", 1), NOW));
    }

    @Test
    void rejectsBlankEntityId() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue("  ", "fg", Map.of("f", 1), NOW));
    }

    @Test
    void rejectsNullFeatureGroup() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue("e1", null, Map.of("f", 1), NOW));
    }

    @Test
    void rejectsBlankFeatureGroup() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue("e1", "  ", Map.of("f", 1), NOW));
    }

    @Test
    void rejectsNullValues() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue("e1", "fg", null, NOW));
    }

    @Test
    void rejectsNullEventTime() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureValue("e1", "fg", Map.of("f", 1), null));
    }

    // ── Defensive copy ──────────────────────────────────────────────

    @Test
    void valuesDefensivelyCopied() {
        var mutable = new HashMap<String, Object>();
        mutable.put("age", 30);
        var fv = new FeatureValue("e1", "fg", mutable, NOW);
        mutable.put("extra", "leaked");
        assertEquals(1, fv.values().size());
    }

    @Test
    void valuesIsUnmodifiable() {
        var fv = sample();
        assertThrows(UnsupportedOperationException.class, () ->
                fv.values().put("new", "value"));
    }

    @Test
    void acceptsEmptyValues() {
        var fv = new FeatureValue("e1", "fg", Map.of(), NOW);
        assertEquals(0, fv.values().size());
    }
}
