package org.pangrova.spi.index.predicate;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link IndexPredicate} sealed interface and all implementations.
 */
class IndexPredicateTest {

    // ── Column validation (shared across all predicates) ──

    @Test
    void nullColumnThrowsForAllPredicates() {
        assertThrows(IllegalArgumentException.class, () -> new EqualTo(null, 1));
        assertThrows(IllegalArgumentException.class, () -> new RangeQuery(null, 1, 10, true, true));
        assertThrows(IllegalArgumentException.class, () -> new InList(null, List.of(1)));
        assertThrows(IllegalArgumentException.class, () -> new TextMatch(null, List.of("a"), TextMatchMode.MATCH_ALL));
        assertThrows(IllegalArgumentException.class, () -> new LikePattern(null, "abc%"));
        assertThrows(IllegalArgumentException.class, () -> new IsNull(null));
        assertThrows(IllegalArgumentException.class, () -> new IsNotNull(null));
        assertThrows(IllegalArgumentException.class,
                () -> new SpatialQuery(null, 0, 0, 1, 1, SpatialOp.INTERSECTS));
        assertThrows(IllegalArgumentException.class,
                () -> new ContainsElement(null, List.of(1), ContainmentMode.CONTAINS_ANY));
    }

    @Test
    void blankColumnThrowsForAllPredicates() {
        assertThrows(IllegalArgumentException.class, () -> new EqualTo("  ", 1));
        assertThrows(IllegalArgumentException.class, () -> new RangeQuery("", 1, 10, true, true));
        assertThrows(IllegalArgumentException.class, () -> new InList(" ", List.of(1)));
        assertThrows(IllegalArgumentException.class, () -> new TextMatch("", List.of("a"), TextMatchMode.MATCH_ALL));
        assertThrows(IllegalArgumentException.class, () -> new LikePattern(" ", "abc%"));
        assertThrows(IllegalArgumentException.class, () -> new IsNull(""));
        assertThrows(IllegalArgumentException.class, () -> new IsNotNull(" "));
        assertThrows(IllegalArgumentException.class,
                () -> new SpatialQuery("", 0, 0, 1, 1, SpatialOp.INTERSECTS));
        assertThrows(IllegalArgumentException.class,
                () -> new ContainsElement(" ", List.of(1), ContainmentMode.CONTAINS_ANY));
    }

    // ── EqualTo ──

    @Nested
    class EqualToTest {

        @Test
        void validConstruction() {
            var p = new EqualTo("col", 42);
            assertEquals("col", p.column());
            assertEquals(42, p.value());
        }

        @Test
        void nullValueThrows() {
            assertThrows(NullPointerException.class, () -> new EqualTo("col", null));
        }

        @Test
        void implementsIndexPredicate() {
            assertInstanceOf(IndexPredicate.class, new EqualTo("col", "v"));
        }
    }

    // ── RangeQuery ──

    @Nested
    class RangeQueryTest {

        @Test
        void validConstruction() {
            var p = new RangeQuery("col", 1, 10, true, false);
            assertEquals("col", p.column());
            assertEquals(1, p.lower());
            assertEquals(10, p.upper());
            assertTrue(p.lowerInclusive());
            assertFalse(p.upperInclusive());
        }

        @Test
        void openEndedLower() {
            var p = new RangeQuery("col", null, 10, false, true);
            assertNull(p.lower());
            assertEquals(10, p.upper());
        }

        @Test
        void openEndedUpper() {
            var p = new RangeQuery("col", 5, null, true, false);
            assertEquals(5, p.lower());
            assertNull(p.upper());
        }

        @Test
        void bothBoundsNullThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> new RangeQuery("col", null, null, true, true));
        }
    }

    // ── InList ──

    @Nested
    class InListTest {

        @Test
        void validConstruction() {
            var p = new InList("col", List.of(1, 2, 3));
            assertEquals("col", p.column());
            assertEquals(List.of(1, 2, 3), p.values());
        }

        @Test
        void defensiveCopy() {
            var original = new ArrayList<Comparable<?>>(List.of(1, 2));
            var p = new InList("col", original);
            original.add(3);
            assertEquals(2, p.values().size(), "modification of original must not affect record");
        }

        @Test
        void immutableValues() {
            var p = new InList("col", List.of(1, 2));
            assertThrows(UnsupportedOperationException.class, () -> p.values().add(3));
        }

        @Test
        void nullValuesThrows() {
            assertThrows(NullPointerException.class, () -> new InList("col", null));
        }

        @Test
        void emptyValuesThrows() {
            assertThrows(IllegalArgumentException.class, () -> new InList("col", List.of()));
        }
    }

    // ── TextMatch ──

    @Nested
    class TextMatchTest {

        @Test
        void validConstruction() {
            var p = new TextMatch("col", List.of("hello", "world"), TextMatchMode.MATCH_ALL);
            assertEquals("col", p.column());
            assertEquals(List.of("hello", "world"), p.tokens());
            assertEquals(TextMatchMode.MATCH_ALL, p.mode());
        }

        @Test
        void defensiveCopy() {
            var original = new ArrayList<>(List.of("a", "b"));
            var p = new TextMatch("col", original, TextMatchMode.MATCH_ANY);
            original.add("c");
            assertEquals(2, p.tokens().size());
        }

        @Test
        void immutableTokens() {
            var p = new TextMatch("col", List.of("a"), TextMatchMode.MATCH_PHRASE);
            assertThrows(UnsupportedOperationException.class, () -> p.tokens().add("b"));
        }

        @Test
        void nullTokensThrows() {
            assertThrows(NullPointerException.class,
                    () -> new TextMatch("col", null, TextMatchMode.MATCH_ALL));
        }

        @Test
        void emptyTokensThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> new TextMatch("col", List.of(), TextMatchMode.MATCH_ALL));
        }

        @Test
        void nullModeThrows() {
            assertThrows(NullPointerException.class,
                    () -> new TextMatch("col", List.of("a"), null));
        }
    }

    // ── LikePattern ──

    @Nested
    class LikePatternTest {

        @Test
        void validConstruction() {
            var p = new LikePattern("col", "abc%");
            assertEquals("col", p.column());
            assertEquals("abc%", p.pattern());
        }

        @Test
        void nullPatternThrows() {
            assertThrows(IllegalArgumentException.class, () -> new LikePattern("col", null));
        }

        @Test
        void blankPatternThrows() {
            assertThrows(IllegalArgumentException.class, () -> new LikePattern("col", "  "));
        }
    }

    // ── IsNull / IsNotNull ──

    @Nested
    class NullPredicateTest {

        @Test
        void isNullValid() {
            var p = new IsNull("col");
            assertEquals("col", p.column());
            assertInstanceOf(IndexPredicate.class, p);
        }

        @Test
        void isNotNullValid() {
            var p = new IsNotNull("col");
            assertEquals("col", p.column());
            assertInstanceOf(IndexPredicate.class, p);
        }
    }

    // ── SpatialQuery ──

    @Nested
    class SpatialQueryTest {

        @Test
        void validConstruction() {
            var p = new SpatialQuery("geo", 0.0, 0.0, 1.0, 1.0, SpatialOp.INTERSECTS);
            assertEquals("geo", p.column());
            assertEquals(0.0, p.minX());
            assertEquals(0.0, p.minY());
            assertEquals(1.0, p.maxX());
            assertEquals(1.0, p.maxY());
            assertEquals(SpatialOp.INTERSECTS, p.op());
        }

        @Test
        void nullOpThrows() {
            assertThrows(NullPointerException.class,
                    () -> new SpatialQuery("geo", 0, 0, 1, 1, null));
        }

        @Test
        void minXExceedsMaxXThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SpatialQuery("geo", 2.0, 0.0, 1.0, 1.0, SpatialOp.WITHIN));
        }

        @Test
        void minYExceedsMaxYThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SpatialQuery("geo", 0.0, 2.0, 1.0, 1.0, SpatialOp.CONTAINS));
        }

        @Test
        void allSpatialOps() {
            for (SpatialOp op : SpatialOp.values()) {
                var p = new SpatialQuery("geo", 0, 0, 1, 1, op);
                assertEquals(op, p.op());
            }
        }
    }

    // ── ContainsElement ──

    @Nested
    class ContainsElementTest {

        @Test
        void validConstruction() {
            var p = new ContainsElement("tags", List.of("a", "b"), ContainmentMode.CONTAINS_ALL);
            assertEquals("tags", p.column());
            assertEquals(List.of("a", "b"), p.elements());
            assertEquals(ContainmentMode.CONTAINS_ALL, p.mode());
        }

        @Test
        void defensiveCopy() {
            var original = new ArrayList<Comparable<?>>(List.of("x"));
            var p = new ContainsElement("tags", original, ContainmentMode.CONTAINS_ANY);
            original.add("y");
            assertEquals(1, p.elements().size());
        }

        @Test
        void immutableElements() {
            var p = new ContainsElement("tags", List.of("a"), ContainmentMode.CONTAINS_ANY);
            assertThrows(UnsupportedOperationException.class, () -> p.elements().add("b"));
        }

        @Test
        void nullElementsThrows() {
            assertThrows(NullPointerException.class,
                    () -> new ContainsElement("tags", null, ContainmentMode.CONTAINS_ANY));
        }

        @Test
        void emptyElementsThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> new ContainsElement("tags", List.of(), ContainmentMode.CONTAINS_ANY));
        }

        @Test
        void nullModeThrows() {
            assertThrows(NullPointerException.class,
                    () -> new ContainsElement("tags", List.of("a"), null));
        }
    }


    // ── Sealed interface permits exactly 9 types ──

    @Test
    void sealedInterfacePermitsExactlyTenTypes() {
        Class<?>[] permitted = IndexPredicate.class.getPermittedSubclasses();
        assertNotNull(permitted);
        assertEquals(10, permitted.length);
    }

    // ── Enum coverage ──

    @Test
    void textMatchModeHasThreeValues() {
        assertEquals(3, TextMatchMode.values().length);
    }

    @Test
    void spatialOpHasThreeValues() {
        assertEquals(3, SpatialOp.values().length);
    }

    @Test
    void containmentModeHasTwoValues() {
        assertEquals(2, ContainmentMode.values().length);
    }
}
