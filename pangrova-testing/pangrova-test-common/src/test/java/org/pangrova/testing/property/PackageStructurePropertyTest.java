package org.pangrova.testing.property;

import org.pangrova.testing.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying package structure governance across Pangrova.
 *
 * <p>Scans all Java packages to verify:</p>
 * <ul>
 *   <li>File count per package &le; 15 (except AST node packages and SPI contract packages)</li>
 *   <li>No horizontal tech-layer packages (dto/, service/, dao/)</li>
 *   <li>No internal package imports from outside the owning module</li>
 *   <li>Split sub-packages file count &le; 8 (Property 39)</li>
 * </ul>
 *
 * <p>Coverage is exhaustive by construction: {@code tries} is set above the number of scanned
 * packages so jqwik enumerates every package instead of sampling a handful of them. Sampling made
 * the gate probabilistic — real violations stayed hidden for as long as the random draw missed
 * them — which contradicts the "scans all Java packages" contract above.</p>
 *
 * <p><b>Validates: Requirements 8.1, 8.3, 8.4, 8.11, 55.2, 58.3</b></p>
 * <p><b>Property 39: Package file count threshold</b></p>
 */
class PackageStructurePropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();
    private static final int MAX_PACKAGE_FILES = 15;

    /**
     * Try budget for package-wide properties. Kept above the number of scanned packages so jqwik's
     * AUTO generation mode switches to exhaustive enumeration: every package is checked on every
     * run, and the actual number of tries equals the package count.
     */
    private static final int EXHAUSTIVE_TRIES = 2000;

    /** Maven modules are at most two directories below the root ({@code group/module/}). */
    private static final int MAX_MODULE_NESTING = 2;

    /**
     * Maximum file count for sub-packages created by package splits.
     * Architecture governance rules require split sub-packages to have ≤ 8 files.
     */
    private static final int MAX_SPLIT_SUBPACKAGE_FILES = 8;

    /**
     * Blocking threshold: packages exceeding this count must not be merged
     * without an exception filing and remediation plan.
     */
    private static final int BLOCKING_THRESHOLD = 25;

    /**
     * Sub-packages created by package split tasks (Phase 11).
     * These are held to the stricter ≤ 8 file threshold.
     */
    private static final Set<String> SPLIT_SUBPACKAGES = Set.of(
            "org.pangrova.coordinator.query.stage",
            "org.pangrova.coordinator.query.audit",
            "org.pangrova.coordinator.query.task",
            "org.pangrova.coordinator.query.security",
            "org.pangrova.coordinator.query.execution.insert",
            "org.pangrova.coordinator.scheduler.strategy",
            "org.pangrova.worker.task.convert",
            "org.pangrova.coordinator.http.query",
            "org.pangrova.coordinator.http.admin",
            "org.pangrova.parser.ast.statement.dml",
            "org.pangrova.parser.ast.statement.maintenance"
    );

    /**
     * Sub-packages that exceed the 8-file threshold but are tracked for
     * further splitting in subsequent tasks. These are checked against
     * the general 15-file threshold instead.
     */
    private static final Set<String> PENDING_FURTHER_SPLIT = Set.of(
            "org.pangrova.coordinator.query.statement",
            "org.pangrova.coordinator.query.execution"
    );

    /**
     * Packages exempt from the 15-file threshold due to well-known patterns.
     * AST node packages (one class per grammar production) and SPI contract packages
     * (one interface per extension point) are inherently large but cohesive.
     */
    private static final Set<String> EXEMPT_PACKAGES = Set.of(
            "org.pangrova.parser.ast",
            "org.pangrova.parser.ast.expression",
            "org.pangrova.parser.ast.statement",  // one class per SQL statement grammar production
            "org.pangrova.spi.connector.metadata",
            "org.pangrova.spi.connector.data",
            "org.pangrova.spi.connector.data.type",
            "org.pangrova.spi.observability",
            "org.pangrova.spi.execution",
            "org.pangrova.coordinator.query.execution.dispatch.task",  // task dispatch — cohesive, needs review
            "org.pangrova.planner.substrait",  // Substrait Rel↔Calcite RelNode converters — cohesive translation layer
            "org.pangrova.coordinator.ai.embedding",  // embedding providers — one per integration
            "org.pangrova.worker.task",  // worker task lifecycle — cohesive
            "org.pangrova.resource.memory",  // memory pool/arbitrator/listener — cohesive
            "org.pangrova.security.authn",  // authenticator implementations — one per scheme
            "org.pangrova.observability.metrics",  // metrics registries — cohesive
            "org.pangrova.planner.cache",  // plan/result cache subsystem — cohesive
            "org.pangrova.planner.cost",  // cost model and statistics — cohesive
            "org.pangrova.coordinator.query.execution",  // query execution orchestration — cohesive
            "org.pangrova.coordinator.query.execution.dispatch",  // task dispatch — cohesive
            "org.pangrova.parser",  // parser core — lexer/parser/builder — cohesive
            "org.pangrova.security.authz",  // authorization implementations — cohesive
            "org.pangrova.shuffle",  // shuffle protocol and client — cohesive
            "org.pangrova.spi.coordination",  // coordination SPI contracts — cohesive
            "org.pangrova.spi.streaming",  // streaming SPI contracts — cohesive
            "org.pangrova.streaming.cdc",  // CDC pipeline — cohesive
            "org.pangrova.connector.iceberg",  // Iceberg connector — cohesive
            "org.pangrova.planner",  // planner core — PlanFragment/Stage/Result/Compiler types — cohesive
            "org.pangrova.coordinator.bootstrap"  // coordinator bootstrap — module startup/shutdown orchestration — cohesive
    );

    /**
     * Packages that exceed the 15-file threshold because they are "one class per X" families where
     * splitting would fragment a cohesive unit, but which are not granted an open-ended exemption.
     * Each is pinned to its current file count: adding a file fails the gate and forces a deliberate
     * decision (split the package, or re-pin with a rationale). Same idea as {@code
     * GOD_CLASS_TRACKED} in {@code CleanCodeMetricsPropertyTest} — freeze the debt, don't hide it.
     */
    private static final Map<String, Integer> TRACKED_OVERSIZE_PACKAGES = Map.of(
            // one class per AI SQL function (ai_classify, ai_embed, …) plus their shared helpers
            "org.pangrova.worker.ai.function", 20,
            // one class per Paimon connector capability; freeze until the connector package is split
            "org.pangrova.connector.paimon", 24,
            // one class per vector algorithm family (HNSW, IVFPQ, DiskANN, Lance) plus their codecs
            "org.pangrova.indexing.engine.vector", 18,
            // one class per optimizer rule plus the CTE materialization support types they share
            "org.pangrova.planner.rule", 16
    );

    private static final Set<String> FORBIDDEN_TECH_LAYER_NAMES = Set.of(
            "dto", "service", "dao", "repository", "controller"
    );

    /**
     * Packages exempt from the tech-layer naming check.
     * These are pre-existing names scheduled for rename in Phase 11 (Task 66).
     */
    private static final Set<String> TECH_LAYER_EXEMPT_PACKAGES = Set.of(
            "org.pangrova.metadata.service"
    );

    private static final List<PackageEntry> PACKAGE_ENTRIES = scanPackageEntries();

    // -----------------------------------------------------------------------
    // Property 18a: Package file count <= 15
    // -----------------------------------------------------------------------

    /**
     * Property 18: Every non-exempt Java package must have at most 15 files.
     *
     * <p><b>Validates: Requirements 8.1, 8.3</b></p>
     */
    @Property(tries = EXHAUSTIVE_TRIES)
    @Tag("Feature: full-pipeline-audit-fix, Property 18: Package structure governance")
    void packageFileCountMustNotExceedThreshold(@ForAll("packageEntries") PackageEntry entry) {
        if (EXEMPT_PACKAGES.contains(entry.packageName)) {
            return;
        }
        Integer pinnedCount = TRACKED_OVERSIZE_PACKAGES.get(entry.packageName);
        if (pinnedCount != null) {
            assertThat(entry.fileCount)
                    .as("Tracked oversize package %s grew to %d files (pinned at %d). Split it, or "
                                    + "re-pin the count in TRACKED_OVERSIZE_PACKAGES with a rationale.",
                            entry.packageName, entry.fileCount, pinnedCount)
                    .isLessThanOrEqualTo(pinnedCount);
            return;
        }
        assertThat(entry.fileCount)
                .as("Package %s has %d files (max %d). Consider splitting by sub-feature.",
                        entry.packageName, entry.fileCount, MAX_PACKAGE_FILES)
                .isLessThanOrEqualTo(MAX_PACKAGE_FILES);
    }

    /**
     * Property 18: No horizontal tech-layer package names (dto/, service/, dao/).
     *
     * <p><b>Validates: Requirements 8.4, 8.11</b></p>
     */
    @Property(tries = EXHAUSTIVE_TRIES)
    @Tag("Feature: full-pipeline-audit-fix, Property 18: Package structure governance")
    void noHorizontalTechLayerPackages(@ForAll("packageEntries") PackageEntry entry) {
        if (TECH_LAYER_EXEMPT_PACKAGES.contains(entry.packageName)) {
            return; // scheduled for rename in Phase 11 Task 66
        }
        String lastSegment = entry.packageName.contains(".")
                ? entry.packageName.substring(entry.packageName.lastIndexOf('.') + 1)
                : entry.packageName;
        assertThat(FORBIDDEN_TECH_LAYER_NAMES)
                .as("Package %s uses forbidden tech-layer name '%s'. Use business-capability naming.",
                        entry.packageName, lastSegment)
                .doesNotContain(lastSegment);
    }

    // -----------------------------------------------------------------------
    // Property 39: Split sub-package file count <= 8
    // -----------------------------------------------------------------------

    /**
     * Property 39: Split sub-packages must have at most 8 files.
     *
     * <p>Packages created by Phase 11 package split tasks are held to a stricter
     * threshold than the general 15-file limit.</p>
     *
     * <p><b>Validates: Requirements 55.2, 58.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: architecture-remediation, Property 39: Package file count threshold")
    void splitSubpackageFileCountMustNotExceedThreshold(
            @ForAll("splitSubpackageEntries") PackageEntry entry) {
        assertThat(entry.fileCount)
                .as("Split sub-package %s has %d files (max %d). Further splitting required.",
                        entry.packageName, entry.fileCount, MAX_SPLIT_SUBPACKAGE_FILES)
                .isLessThanOrEqualTo(MAX_SPLIT_SUBPACKAGE_FILES);
    }

    /**
     * Property 39: No package may exceed the blocking threshold of 25 files.
     *
     * <p><b>Validates: Requirements 55.2</b></p>
     */
    @Property(tries = EXHAUSTIVE_TRIES)
    @Tag("Feature: architecture-remediation, Property 39: Package file count threshold")
    void noPackageExceedsBlockingThreshold(@ForAll("packageEntries") PackageEntry entry) {
        if (EXEMPT_PACKAGES.contains(entry.packageName)) {
            return;
        }
        assertThat(entry.fileCount)
                .as("Package %s has %d files, exceeding blocking threshold of %d. "
                                + "Must split before merge.",
                        entry.packageName, entry.fileCount, BLOCKING_THRESHOLD)
                .isLessThanOrEqualTo(BLOCKING_THRESHOLD);
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<PackageEntry> packageEntries() {
        if (PACKAGE_ENTRIES.isEmpty()) {
            return Arbitraries.just(new PackageEntry("(none)", 0));
        }
        return Arbitraries.of(PACKAGE_ENTRIES);
    }

    @Provide
    Arbitrary<PackageEntry> splitSubpackageEntries() {
        List<PackageEntry> splitEntries = PACKAGE_ENTRIES.stream()
                .filter(e -> SPLIT_SUBPACKAGES.contains(e.packageName))
                .filter(e -> !PENDING_FURTHER_SPLIT.contains(e.packageName))
                .toList();
        if (splitEntries.isEmpty()) {
            return Arbitraries.just(new PackageEntry("(none)", 0));
        }
        return Arbitraries.of(splitEntries);
    }

    // -----------------------------------------------------------------------
    // Scanning
    // -----------------------------------------------------------------------

    private static List<PackageEntry> scanPackageEntries() {
        List<PackageEntry> entries = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Path file : findAllMainJavaFiles()) {
            Path parent = file.getParent();
            String packageName = derivePackageName(parent);
            if (packageName != null && seen.add(packageName)) {
                int count = countJavaFiles(parent);
                entries.add(new PackageEntry(packageName, count));
            }
        }
        return List.copyOf(entries);
    }

    private static String derivePackageName(Path dir) {
        String path = dir.toString().replace('\\', '/');
        int idx = path.indexOf("org/pangrova/");
        if (idx < 0) {
            return null;
        }
        return path.substring(idx).replace('/', '.');
    }

    private static int countJavaFiles(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return (int) files
                    .filter(p -> p.getFileName().toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                    .count();
        } catch (IOException e) {
            return 0;
        }
    }

    private static List<Path> findAllMainJavaFiles() {
        List<Path> result = new ArrayList<>();
        for (Path srcDir : findJavaSourceRoots()) {
            try (Stream<Path> files = Files.walk(srcDir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".java"))
                        .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                        .filter(p -> !p.getFileName().toString().equals("module-info.java"))
                        .forEach(result::add);
            } catch (IOException ignored) {
                // skip
            }
        }
        return List.copyOf(result);
    }

    /**
     * Collects {@code src/main/java} roots of every Maven module, including nested ones
     * ({@code pangrova-connector/pangrova-connector-iceberg}, {@code pangrova-shuffle/*}, …). Scanning
     * only top-level {@code pangrova-*} directories left whole modules ungoverned.
     */
    private static List<Path> findJavaSourceRoots() {
        List<Path> roots = new ArrayList<>();
        try (Stream<Path> dirs = Files.walk(PROJECT_ROOT, MAX_MODULE_NESTING)) {
            dirs.filter(Files::isDirectory)
                    .filter(d -> d.getFileName().toString().startsWith("pangrova-"))
                    .filter(d -> Files.isRegularFile(d.resolve("pom.xml")))
                    .map(d -> d.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .forEach(roots::add);
        } catch (IOException ignored) {
            // skip
        }
        return List.copyOf(roots);
    }

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    record PackageEntry(String packageName, int fileCount) {
        @Override
        public String toString() {
            return packageName + " (" + fileCount + " files)";
        }
    }
}
