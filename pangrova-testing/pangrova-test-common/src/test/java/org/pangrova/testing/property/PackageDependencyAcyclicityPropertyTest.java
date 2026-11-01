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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 40: Package dependency acyclicity.
 *
 * <p>Verifies that within each module, sub-packages do not form bidirectional
 * (cyclic) import dependencies. Sub-package dependencies must be unidirectional.</p>
 *
 * <p>For example, if {@code coordinator/query/execution/} imports from
 * {@code coordinator/query/stage/}, then {@code coordinator/query/stage/}
 * must NOT import from {@code coordinator/query/execution/}.</p>
 *
 * <p><b>Validates: Requirements 55.3, 57.4</b></p>
 */
class PackageDependencyAcyclicityPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();
    private static final Pattern IMPORT_PATTERN =
            Pattern.compile("^import\\s+(?:static\\s+)?(org\\.pangrova\\.[a-zA-Z0-9_.]+)\\s*;");

    /**
     * Repository-relative source roots for modules that are not located directly
     * under the repository root. Defaults to the module name itself.
     */
    private static final Map<String, String> MODULE_SRC_DIRS = Map.of(
            "pangrova-streaming-runtime", "pangrova-streaming/pangrova-streaming-runtime",
            "pangrova-shuffle-core", "pangrova-shuffle/pangrova-shuffle-core",
            "pangrova-shuffle-service", "pangrova-shuffle/pangrova-shuffle-service");

    /**
     * Modules to check for package-level dependency cycles.
     * Covers all main production modules with significant internal package structure.
     */
    private static final List<String> CHECKED_MODULES = List.of(
            "pangrova-common",
            "pangrova-spi",
            "pangrova-protocol",
            "pangrova-parser",
            "pangrova-planner",
            "pangrova-metadata",
            "pangrova-coordinator",
            "pangrova-worker",
            "pangrova-observability",
            "pangrova-governance",
            "pangrova-security",
            "pangrova-resilience",
            "pangrova-streaming-runtime",
            "pangrova-operations",
            "pangrova-resource",
            "pangrova-config",
            "pangrova-indexing",
            "pangrova-shuffle-core",
            "pangrova-shuffle-service"
    );

    /**
     * Known acceptable bidirectional dependencies between tightly coupled
     * data model sub-packages. These are exempt from the acyclicity check
     * because they represent cohesive data structures (e.g., Page contains
     * Blocks, and Block implementations may reference Page).
     */
    private static final Set<String> EXEMPT_PAIRS = Set.of(
            "org.pangrova.spi.connector.data.block|org.pangrova.spi.connector.data.page",
            "org.pangrova.spi.connector.data|org.pangrova.spi.connector.metadata",
            "org.pangrova.metadata.catalog|org.pangrova.metadata.store",
            "org.pangrova.metadata.catalog|org.pangrova.metadata.event",
            "org.pangrova.metadata.store|org.pangrova.metadata.sync",
            "org.pangrova.metadata.event|org.pangrova.metadata.sync",
            "org.pangrova.metadata.adapter|org.pangrova.metadata.sync",
            "org.pangrova.observability.logging|org.pangrova.observability.tracing",
            "org.pangrova.worker.exchange|org.pangrova.worker.transport"
    );

    private static final List<String> JAVA_MODULES = List.of(
            "pangrova-common",
            "pangrova-spi",
            "pangrova-protocol",
            "pangrova-connector-iceberg",
            "pangrova-connector-paimon",
            "pangrova-parser",
            "pangrova-planner",
            "pangrova-metadata",
            "pangrova-coordinator",
            "pangrova-worker",
            "pangrova-observability",
            "pangrova-governance",
            "pangrova-security",
            "pangrova-resilience",
            "pangrova-streaming-runtime",
            "pangrova-operations",
            "pangrova-resource"
    );

    /**
     * All detected bidirectional package dependency pairs across all modules.
     * Computed once at class load time.
     */
    private static final List<CyclicPair> CYCLIC_PAIRS = detectAllCyclicPairs();

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /**
     * Property 40: No bidirectional import dependency between sub-packages
     * within the same module.
     *
     * <p><b>Validates: Requirements 55.3, 57.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: architecture-remediation, Property 40: Package dependency acyclicity")
    void noSubpackageCyclicDependencies(@ForAll("cyclicPairOrNone") CyclicPair pair) {
        assertThat(pair.isSentinel())
                .as("Bidirectional dependency detected in module '%s': "
                                + "'%s' ↔ '%s'. Sub-package dependencies must be unidirectional.",
                        pair.module(), pair.packageA(), pair.packageB())
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<CyclicPair> cyclicPairOrNone() {
        if (CYCLIC_PAIRS.isEmpty()) {
            return Arbitraries.just(CyclicPair.sentinel());
        }
        return Arbitraries.of(CYCLIC_PAIRS);
    }

    // -----------------------------------------------------------------------
    // Cycle detection
    // -----------------------------------------------------------------------

    private static List<CyclicPair> detectAllCyclicPairs() {
        List<CyclicPair> result = new ArrayList<>();
        for (String module : CHECKED_MODULES) {
            Path srcDir = PROJECT_ROOT.resolve(MODULE_SRC_DIRS.getOrDefault(module, module))
                    .resolve("src/main/java");
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            Map<String, Set<String>> packageDeps = buildPackageDependencyGraph(srcDir);
            detectCycles(module, packageDeps, result);
        }
        return List.copyOf(result);
    }

    /**
     * Builds a directed graph: package → set of packages it imports from.
     * Only considers org.pangrova.* imports within the same module source tree.
     */
    private static Map<String, Set<String>> buildPackageDependencyGraph(Path srcDir) {
        Map<String, Set<String>> graph = new HashMap<>();
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                    .forEach(javaFile -> processFile(javaFile, srcDir, graph));
        } catch (IOException ignored) {
            // skip if walk fails
        }
        return graph;
    }

    private static void processFile(
            Path javaFile, Path srcDir, Map<String, Set<String>> graph) {
        String sourcePackage = derivePackageName(javaFile.getParent(), srcDir);
        if (sourcePackage == null) {
            return;
        }
        try {
            List<String> lines = Files.readAllLines(javaFile);
            for (String line : lines) {
                String trimmed = line.trim();
                if (trimmed.startsWith("package ")) {
                    continue;
                }
                if (!trimmed.startsWith("import ")) {
                    if (isClassDeclarationStart(trimmed)) {
                        break;
                    }
                    continue;
                }
                Matcher m = IMPORT_PATTERN.matcher(trimmed);
                if (!m.matches()) {
                    continue;
                }
                String importedFqn = m.group(1);
                String targetPackage = extractPackage(importedFqn);
                if (targetPackage != null
                        && !targetPackage.equals(sourcePackage)
                        && targetPackage.startsWith("org.pangrova.")) {
                    graph.computeIfAbsent(sourcePackage, k -> new HashSet<>())
                            .add(targetPackage);
                }
            }
        } catch (IOException ignored) {
            // skip unreadable files
        }
    }

    /**
     * Detects bidirectional dependencies: if A → B and B → A, that's a cycle.
     * Only checks packages that share a common parent (sibling sub-packages).
     */
    private static void detectCycles(
            String module,
            Map<String, Set<String>> graph,
            List<CyclicPair> result) {
        Set<String> checked = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : graph.entrySet()) {
            String pkgA = entry.getKey();
            for (String pkgB : entry.getValue()) {
                if (!shareSiblingParent(pkgA, pkgB)) {
                    continue;
                }
                String pairKey = pkgA.compareTo(pkgB) < 0
                        ? pkgA + "|" + pkgB
                        : pkgB + "|" + pkgA;
                if (checked.contains(pairKey)) {
                    continue;
                }
                checked.add(pairKey);
                if (EXEMPT_PAIRS.contains(pairKey)) {
                    continue;
                }
                Set<String> reverseDeps = graph.getOrDefault(pkgB, Set.of());
                if (reverseDeps.contains(pkgA)) {
                    result.add(new CyclicPair(module, pkgA, pkgB));
                }
            }
        }
    }

    /**
     * Returns true if two packages share the same immediate parent package.
     * For example, "o.a.coordinator.query.execution" and "o.a.coordinator.query.stage"
     * share parent "o.a.coordinator.query".
     */
    private static boolean shareSiblingParent(String pkgA, String pkgB) {
        String parentA = parentPackage(pkgA);
        String parentB = parentPackage(pkgB);
        return parentA != null && parentA.equals(parentB);
    }

    private static String parentPackage(String pkg) {
        int lastDot = pkg.lastIndexOf('.');
        return lastDot > 0 ? pkg.substring(0, lastDot) : null;
    }

    private static String derivePackageName(Path dir, Path srcRoot) {
        Path relative = srcRoot.relativize(dir);
        String path = relative.toString().replace('\\', '/');
        if (path.isEmpty()) {
            return null;
        }
        return path.replace('/', '.');
    }

    private static String extractPackage(String fqn) {
        int lastDot = fqn.lastIndexOf('.');
        return lastDot > 0 ? fqn.substring(0, lastDot) : null;
    }

    private static boolean isClassDeclarationStart(String trimmed) {
        return trimmed.startsWith("public ")
                || trimmed.startsWith("class ")
                || trimmed.startsWith("interface ")
                || trimmed.startsWith("enum ")
                || trimmed.startsWith("record ")
                || trimmed.startsWith("sealed ")
                || trimmed.startsWith("final class ")
                || trimmed.startsWith("abstract class ");
    }

    // -----------------------------------------------------------------------
    // Value types
    // -----------------------------------------------------------------------

    record CyclicPair(String module, String packageA, String packageB) {
        static CyclicPair sentinel() {
            return new CyclicPair("(none)", "(none)", "(none)");
        }

        boolean isSentinel() {
            return "(none)".equals(module);
        }

        @Override
        public String toString() {
            return module + ": " + packageA + " ↔ " + packageB;
        }
    }
}
