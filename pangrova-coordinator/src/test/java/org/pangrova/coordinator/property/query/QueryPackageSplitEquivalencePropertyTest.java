package org.pangrova.coordinator.property.query;

import org.pangrova.testing.TestProjectPaths;

import net.jqwik.api.Example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the query package ownership layout exhaustively.
 * Behavioral equivalence is covered by the query lifecycle and execution suites,
 * not by source locations or arbitrary package file-count limits.
 */
class QueryPackageSplitEquivalencePropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();
    private static final Path QUERY_SRC = PROJECT_ROOT
            .resolve("pangrova-coordinator/src/main/java/org/pangrova/coordinator/query");

    /**
     * Expected sub-packages and their required classes after the split.
     * Each sub-package must contain at least these classes.
     */
    private static final Map<String, Set<String>> EXPECTED_SUBPACKAGE_CLASSES = Map.of(
            "execution", Set.of(
                    "QueryDispatcher.java",
                    "QueryDispatcherContext.java"),
            "execution/split", Set.of(
                    "SplitFetcher.java"),
            "session", Set.of(
                    "CoordinatorSessionContext.java",
                    "SessionContextConverter.java"),
            "stage", Set.of(
                    "StageNode.java",
                    "StageStateMachine.java"),
            "statement", Set.of(
                    "DdlExecutor.java",
                    "ShowDescribeExecutor.java",
                    "StatementClassifier.java"),
            "statement/dml", Set.of(
                    "DeleteFilterConverter.java"),
            "audit", Set.of(
                    "QueryAuditEmitter.java"),
            "security", Set.of(
                    "AccessControlEnforcer.java")
    );

    /**
     * Classes that must remain in the root query/ package (lifecycle core).
     */
    private static final Set<String> EXPECTED_ROOT_CLASSES = Set.of(
            "QueryManager.java",
            "QueryStateMachine.java"
    );

    /**
     * All structural checks, evaluated on every run.
     */
    private static final List<StructuralCheck> CHECKS = buildChecks();

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    @Example
    void expectedClassesExistInDesignatedSubpackages() {
        assertThat(QUERY_SRC).isDirectory();
        assertThat(CHECKS).isNotEmpty().allSatisfy(check ->
                assertThat(check.satisfied()).as(check.description()).isTrue());
    }

    // -----------------------------------------------------------------------
    // Check building
    // -----------------------------------------------------------------------

    private static List<StructuralCheck> buildChecks() {
        List<StructuralCheck> checks = new ArrayList<>();
        // Check sub-package classes exist
        for (Map.Entry<String, Set<String>> entry : EXPECTED_SUBPACKAGE_CLASSES.entrySet()) {
            String subPkg = entry.getKey();
            Path subDir = QUERY_SRC.resolve(subPkg);
            for (String className : entry.getValue()) {
                boolean exists = Files.isRegularFile(subDir.resolve(className));
                checks.add(new StructuralCheck(
                        String.format("Class '%s' must exist in query/%s/", className, subPkg),
                        exists));
            }
        }

        // Check root package classes exist
        for (String className : EXPECTED_ROOT_CLASSES) {
            boolean exists = Files.isRegularFile(QUERY_SRC.resolve(className));
            checks.add(new StructuralCheck(
                    String.format("Class '%s' must exist in query/ root package", className),
                    exists));
        }

        // Check each sub-package directory exists
        for (String subPkg : EXPECTED_SUBPACKAGE_CLASSES.keySet()) {
            boolean exists = Files.isDirectory(QUERY_SRC.resolve(subPkg));
            checks.add(new StructuralCheck(
                    String.format("Sub-package query/%s/ must exist", subPkg),
                    exists));
        }

        return List.copyOf(checks);
    }

    // -----------------------------------------------------------------------
    // Value types
    // -----------------------------------------------------------------------

    record StructuralCheck(String description, boolean satisfied) {
        @Override
        public String toString() {
            return (satisfied ? "✓ " : "✗ ") + description;
        }
    }
}
