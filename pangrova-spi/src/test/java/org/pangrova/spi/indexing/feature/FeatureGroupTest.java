package org.pangrova.spi.indexing.feature;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link FeatureGroup}.
 */
class FeatureGroupTest {

    private static FeatureGroup sample() {
        return new FeatureGroup(
                "user_profile", "User profile features",
                "user_id", List.of("age", "country", "signup_days"),
                "event_time");
    }

    // ── Field accessors ─────────────────────────────────────────────

    @Test
    void fieldAccessorsReturnConstructorValues() {
        var fg = sample();
        assertEquals("user_profile", fg.name());
        assertEquals("User profile features", fg.description());
        assertEquals("user_id", fg.entityIdColumn());
        assertEquals(List.of("age", "country", "signup_days"), fg.featureNames());
        assertEquals("event_time", fg.timestampColumn());
    }

    @Test
    void nullTimestampColumnIsAllowed() {
        var fg = new FeatureGroup("fg", "desc", "id", List.of("f1"), null);
        assertNull(fg.timestampColumn());
    }

    @Test
    void nullDescriptionDefaultsToEmpty() {
        var fg = new FeatureGroup("fg", null, "id", List.of("f1"), null);
        assertEquals("", fg.description());
    }

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void rejectsNullName() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup(null, "desc", "id", List.of("f1"), null));
    }

    @Test
    void rejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup("  ", "desc", "id", List.of("f1"), null));
    }

    @Test
    void rejectsNullEntityIdColumn() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup("fg", "desc", null, List.of("f1"), null));
    }

    @Test
    void rejectsBlankEntityIdColumn() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup("fg", "desc", "  ", List.of("f1"), null));
    }

    @Test
    void rejectsNullFeatureNames() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup("fg", "desc", "id", null, null));
    }

    @Test
    void rejectsEmptyFeatureNames() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureGroup("fg", "desc", "id", List.of(), null));
    }

    // ── Defensive copy ──────────────────────────────────────────────

    @Test
    void featureNamesDefensivelyCopied() {
        var mutable = new ArrayList<>(List.of("f1", "f2"));
        var fg = new FeatureGroup("fg", "desc", "id", mutable, null);
        mutable.add("f3");
        assertEquals(2, fg.featureNames().size());
    }

    @Test
    void featureNamesIsUnmodifiable() {
        var fg = sample();
        assertThrows(UnsupportedOperationException.class, () ->
                fg.featureNames().add("new_feature"));
    }
}
