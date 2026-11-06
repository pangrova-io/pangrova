package org.pangrova.coordinator.property.query;

import org.pangrova.testing.testkit.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.Example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 38: Package split behavioral equivalence.
 *
 * <p>Verifies that after splitting {@code coordinator/query/} into sub-packages,
 * all expected classes exist in their designated modules and packages. Query state,
 * physical cleanup and retention belong to Query Core, not the Coordinator.</p>
 *
 * <p>Behavioral equivalence is ultimately verified by the full test suite passing.
 * This property test validates the structural invariants that guarantee equivalence:
 * every class that existed before the split still exists in a well-defined location.</p>
 *
 * <p><b>Validates: Requirements 55.1</b></p>
 */
class QueryPackageSplitEquivalencePropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();
    private static final Path QUERY_SRC = PROJECT_ROOT
            .resolve("pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query");

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
            "stage", Set.of(
                    "StageNode.java"),
            "statement", Set.of(
                    "DdlExecutor.java",
                    "ShowDescribeExecutor.java"),
            "statement/dml", Set.of(
                    "DeleteFilterConverter.java"),
            "audit", Set.of(
                    "QueryAuditEmitter.java"),
            "security", Set.of(
                    "AccessControlEnforcer.java")
    );

    /**
     * Remaining Coordinator orchestration, pending its separate domain migration.
     */
    private static final Set<String> EXPECTED_ROOT_CLASSES = Set.of(
            "QueryManager.java"
    );

    private static final Map<String, String> DOMAIN_OWNERS = Map.of(
            "StatementClassifier.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/statement",
            "StatementTableTargets.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/statement",
            "SessionTableNames.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/session",
            "StatementCategory.java", "pangrova-query-api/src/main/java/org/pangrova/query/api/statement",
            "QueryStateMachine.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/lifecycle",
            "QueryConnectorAuthority.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/lifecycle",
            "QueryTerminalCleanup.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/lifecycle",
            "QueryRetentionScheduler.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/retention",
            "StageStateMachine.java", "pangrova-query-core/src/main/java/org/pangrova/query/core/stage",
            "QuerySession.java", "pangrova-query-api/src/main/java/org/pangrova/query/api/session");

    /**
     * Maximum files allowed in the root query/ package (excluding sub-packages).
     * This existing cap is not evidence of semantic separation; explicit owners below
     * also reject duplicate state/cleanup classes in the Coordinator.
     */
    private static final int MAX_ROOT_PACKAGE_FILES = 12;

    /**
     * All structural checks as a flat list for property-based sampling.
     */
    private static final List<StructuralCheck> CHECKS = buildChecks();

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /**
     * Property 38: Every expected class exists in its designated sub-package
     * after the coordinator/query/ split.
     *
     * <p><b>Validates: Requirements 55.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: architecture-remediation, Property 38: Package split equivalence")
    void expectedClassesExistInDesignatedSubpackages(
            @ForAll("structuralChecks") StructuralCheck check) {
        assertThat(check.satisfied())
                .as(check.description())
                .isTrue();
    }

    @Example
    void everyStructuralBoundaryIsCheckedDeterministically() {
        assertThat(CHECKS).allSatisfy(check -> assertThat(check.satisfied())
                .as(check.description()).isTrue());
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<StructuralCheck> structuralChecks() {
        return Arbitraries.of(CHECKS);
    }

    // -----------------------------------------------------------------------
    // Check building
    // -----------------------------------------------------------------------

    private static List<StructuralCheck> buildChecks() {
        List<StructuralCheck> checks = new ArrayList<>();
        checks.add(new StructuralCheck("Coordinator query source must exist", Files.isDirectory(QUERY_SRC)));
        if (!Files.isDirectory(QUERY_SRC)) {
            return List.copyOf(checks);
        }

        for (var entry : DOMAIN_OWNERS.entrySet()) {
            Path owner = PROJECT_ROOT.resolve("pangrova-query").resolve(entry.getValue()).resolve(entry.getKey());
            checks.add(new StructuralCheck("Unique domain owner: " + owner, Files.isRegularFile(owner)));
            try (Stream<Path> files = Files.walk(QUERY_SRC)) {
                checks.add(new StructuralCheck("No Coordinator duplicate: " + entry.getKey(),
                        files.noneMatch(path -> path.getFileName().toString().equals(entry.getKey()))));
            } catch (IOException failure) {
                throw new java.io.UncheckedIOException(failure);
            }
        }
        checks.add(new StructuralCheck("Old retention scheduler must be removed",
                !Files.exists(QUERY_SRC.resolve("QueryCleanupScheduler.java"))));

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

        // Check root package file count
        int rootFileCount = countDirectJavaFiles(QUERY_SRC);
        checks.add(new StructuralCheck(
                String.format("Root query/ package has %d files (max %d)",
                        rootFileCount, MAX_ROOT_PACKAGE_FILES),
                rootFileCount <= MAX_ROOT_PACKAGE_FILES));

        // Check each sub-package directory exists
        for (String subPkg : EXPECTED_SUBPACKAGE_CLASSES.keySet()) {
            boolean exists = Files.isDirectory(QUERY_SRC.resolve(subPkg));
            checks.add(new StructuralCheck(
                    String.format("Sub-package query/%s/ must exist", subPkg),
                    exists));
        }

        return List.copyOf(checks);
    }

    private static int countDirectJavaFiles(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return (int) files
                    .filter(p -> p.getFileName().toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                    .count();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
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
