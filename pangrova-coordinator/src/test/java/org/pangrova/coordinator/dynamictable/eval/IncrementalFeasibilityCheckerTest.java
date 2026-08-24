package org.pangrova.coordinator.dynamictable.eval;

import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker.FeasibilityResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker.QueryFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IncrementalFeasibilityChecker}.
 *
 * <p>Covers the full operator feasibility matrix (R11 AC3).</p>
 *
 * <p><b>Validates: Requirements 11</b></p>
 */
class IncrementalFeasibilityCheckerTest {

    private IncrementalFeasibilityChecker checker;

    @BeforeEach
    void setUp() {
        checker = new IncrementalFeasibilityChecker();
    }

    @Test
    void filterOnlyIsFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.FILTER));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void projectionOnlyIsFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.PROJECTION));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void innerJoinIsFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.INNER_JOIN));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void groupByIsFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.GROUP_BY));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void unionAllIsFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.UNION_ALL));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void windowFunctionIsNotFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.WINDOW));
        assertFalse(result.feasible());
        assertNotNull(result.reason());
        assertTrue(result.reason().contains("WINDOW"));
    }

    @Test
    void nonEquiJoinIsNotFeasible() {
        FeasibilityResult result = checker.check(Set.of(QueryFeature.NON_EQUI_JOIN));
        assertFalse(result.feasible());
        assertNotNull(result.reason());
        assertTrue(result.reason().contains("non-equi JOIN"));
    }

    @Test
    void distinctWithoutGroupByIsNotFeasible() {
        FeasibilityResult result = checker.check(
                Set.of(QueryFeature.DISTINCT_WITHOUT_GROUP_BY));
        assertFalse(result.feasible());
        assertNotNull(result.reason());
        assertTrue(result.reason().contains("DISTINCT"));
    }

    @Test
    void combinedSupportedFeaturesAreFeasible() {
        FeasibilityResult result = checker.check(EnumSet.of(
                QueryFeature.FILTER,
                QueryFeature.PROJECTION,
                QueryFeature.INNER_JOIN,
                QueryFeature.GROUP_BY,
                QueryFeature.UNION_ALL));
        assertTrue(result.feasible());
        assertNull(result.reason());
    }

    @Test
    void mixedSupportedAndUnsupportedIsNotFeasible() {
        FeasibilityResult result = checker.check(EnumSet.of(
                QueryFeature.FILTER,
                QueryFeature.WINDOW));
        assertFalse(result.feasible());
        assertNotNull(result.reason());
        assertTrue(result.reason().contains("WINDOW"));
    }

    @Test
    void multipleUnsupportedFeaturesListsAll() {
        FeasibilityResult result = checker.check(EnumSet.of(
                QueryFeature.WINDOW,
                QueryFeature.NON_EQUI_JOIN,
                QueryFeature.DISTINCT_WITHOUT_GROUP_BY));
        assertFalse(result.feasible());
        assertTrue(result.reason().contains("WINDOW"));
        assertTrue(result.reason().contains("non-equi JOIN"));
        assertTrue(result.reason().contains("DISTINCT"));
    }

    @Test
    void emptyFeatureSetIsFeasible() {
        FeasibilityResult result = checker.check(Set.of());
        assertTrue(result.feasible());
        assertNull(result.reason());
    }
}
