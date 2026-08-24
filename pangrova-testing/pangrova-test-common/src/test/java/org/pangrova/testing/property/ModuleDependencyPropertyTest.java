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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying module dependency direction correctness.
 *
 * <p>Scans all module {@code src/main/java} directories for import statements,
 * builds a dependency graph, and verifies it against the architecture hard rules
 * defined in {@code architecture-governance-rules.md}.</p>
 *
 * <p>Post-consolidation: validates the 18-module structure after merging
 * pangrova-connector → pangrova-spi, pangrova-substrait → pangrova-planner,
 * pangrova-tenant → pangrova-security, pangrova-transport → pangrova-spi + pangrova-worker.</p>
 *
 * <p><b>Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9</b></p>
 */
class ModuleDependencyPropertyTest {

    /**
     * Maps a Java package prefix to its owning module name.
     * Order matters: more specific prefixes must come first.
     *
     * <p>Post-consolidation (18 modules): pangrova-connector merged into pangrova-spi,
     * pangrova-substrait merged into pangrova-planner, pangrova-tenant merged into
     * pangrova-security, pangrova-transport split into pangrova-spi (contracts) and
     * pangrova-worker (implementation).</p>
     */
    private static final List<PackageModuleMapping> PACKAGE_TO_MODULE = List.of(
            // Connector implementations (most specific first)
            new PackageModuleMapping("org.pangrova.connector.iceberg", "pangrova-connector-iceberg"),
            new PackageModuleMapping("org.pangrova.connector.paimon", "pangrova-connector-paimon"),
            // Consolidated: connector base → pangrova-spi
            new PackageModuleMapping("org.pangrova.spi.connector.base", "pangrova-spi"),
            // Consolidated: transport contracts → pangrova-spi
            new PackageModuleMapping("org.pangrova.spi.transport", "pangrova-spi"),
            // Consolidated: transport implementation → pangrova-worker
            new PackageModuleMapping("org.pangrova.worker.transport", "pangrova-worker"),
            // Consolidated: substrait → pangrova-planner
            new PackageModuleMapping("org.pangrova.planner.substrait", "pangrova-planner"),
            // Consolidated: tenant → pangrova-security
            new PackageModuleMapping("org.pangrova.security.tenant", "pangrova-security"),
            // Standard module mappings
            new PackageModuleMapping("org.pangrova.coordinator", "pangrova-coordinator"),
            new PackageModuleMapping("org.pangrova.worker", "pangrova-worker"),
            new PackageModuleMapping("org.pangrova.parser", "pangrova-parser"),
            new PackageModuleMapping("org.pangrova.planner", "pangrova-planner"),
            new PackageModuleMapping("org.pangrova.metadata", "pangrova-metadata"),
            new PackageModuleMapping("org.pangrova.protocol", "pangrova-protocol"),
            new PackageModuleMapping("org.pangrova.spi", "pangrova-spi"),
            new PackageModuleMapping("org.pangrova.common", "pangrova-common"),
            new PackageModuleMapping("org.pangrova.observability", "pangrova-observability"),
            new PackageModuleMapping("org.pangrova.governance", "pangrova-governance"),
            new PackageModuleMapping("org.pangrova.security", "pangrova-security"),
            new PackageModuleMapping("org.pangrova.resilience", "pangrova-resilience"),
            new PackageModuleMapping("org.pangrova.streaming", "pangrova-streaming"),
            new PackageModuleMapping("org.pangrova.operations", "pangrova-operations"),
            new PackageModuleMapping("org.pangrova.resource", "pangrova-resource"),
            new PackageModuleMapping("org.pangrova.testing", "pangrova-testing")
    );

    /**
     * Allowed dependencies per module. A module may only import from the listed targets.
     * Modules not listed here have no restrictions enforced by this test (e.g. pangrova-coordinator
     * is an integration hub with broad allowed dependencies).
     *
     * <p>Post-consolidation (18 modules): removed pangrova-connector, pangrova-substrait,
     * pangrova-tenant, pangrova-transport. Updated allowed deps for consolidated modules.</p>
     */
    private static final Map<String, Set<String>> ALLOWED_DEPS = Map.ofEntries(
            // Leaf module: no pangrova dependencies
            Map.entry("pangrova-common", Set.of()),
            // Stable contract layers: pangrova-spi now includes connector base + transport contracts
            Map.entry("pangrova-spi", Set.of("pangrova-common")),
            Map.entry("pangrova-protocol", Set.of("pangrova-common", "pangrova-spi")),
            // Connector implementations: only SPI + common (connector base is now in SPI)
            Map.entry("pangrova-connector-iceberg", Set.of("pangrova-spi", "pangrova-common")),
            Map.entry("pangrova-connector-paimon", Set.of("pangrova-spi", "pangrova-common")),
            // Cross-cutting providers: must NOT depend on execution modules
            Map.entry("pangrova-observability", Set.of("pangrova-spi", "pangrova-common")),
            Map.entry("pangrova-governance", Set.of(
                    "pangrova-spi", "pangrova-common")),
            // pangrova-security now includes tenant
            Map.entry("pangrova-security", Set.of("pangrova-spi", "pangrova-common")),
            Map.entry("pangrova-resilience", Set.of("pangrova-common", "pangrova-spi")),
            // Ops/resource: must NOT depend on coordinator/worker internals
            Map.entry("pangrova-operations", Set.of("pangrova-common", "pangrova-spi")),
            Map.entry("pangrova-resource", Set.of("pangrova-common", "pangrova-spi")),
            // Streaming: only SPI + common (connector base is now in SPI)
            Map.entry("pangrova-streaming", Set.of("pangrova-spi", "pangrova-common")),
            // Worker: includes transport; depends on SPI, protocol, common, cross-cutting
            Map.entry("pangrova-worker", Set.of(
                    "pangrova-spi", "pangrova-protocol", "pangrova-common",
                    "pangrova-observability", "pangrova-resilience",
                    "pangrova-resource", "pangrova-security", "pangrova-operations"))
    );

    /** Modules whose imports are checked (those with explicit allowed-deps rules). */
    private static final List<String> CONSTRAINED_MODULES = List.copyOf(ALLOWED_DEPS.keySet());

    private static final Pattern IMPORT_PATTERN =
            Pattern.compile("^import\\s+(?:static\\s+)?(org\\.pangrova\\.[a-zA-Z0-9_.]+)\\s*;");

    /**
     * Lazily computed dependency violations: module → list of violation descriptions.
     * Computed once and shared across all property invocations.
     */
    private static final Map<String, List<String>> VIOLATIONS = computeAllViolations();

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /**
     * Property 10: For any constrained module, no import violates the allowed dependency rules.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void noForbiddenDependencies(@ForAll("constrainedModules") String module) {
        List<String> moduleViolations = VIOLATIONS.getOrDefault(module, List.of());
        assertThat(moduleViolations)
                .as("Module '%s' has forbidden dependency violations", module)
                .isEmpty();
    }

    /**
     * Property 10: pangrova-spi must not depend on any implementation module.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void spiNeverDependsOnImplementation(@ForAll("spiImportedModules") String depModule) {
        Set<String> allowed = Set.of("pangrova-common");
        assertThat(allowed)
                .as("pangrova-spi must not depend on '%s'", depModule)
                .contains(depModule);
    }

    /**
     * Property 10: pangrova-protocol must not depend on any implementation module.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void protocolNeverDependsOnImplementation(@ForAll("protocolImportedModules") String depModule) {
        Set<String> allowed = Set.of("pangrova-common", "pangrova-spi");
        assertThat(allowed)
                .as("pangrova-protocol must not depend on '%s'", depModule)
                .contains(depModule);
    }

    /**
     * Property 10: pangrova-streaming must not depend on pangrova-coordinator internals.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void streamingNeverDependsOnCoordinator(@ForAll("streamingImportedModules") String depModule) {
        assertThat(depModule)
                .as("pangrova-streaming must not depend on pangrova-coordinator")
                .isNotEqualTo("pangrova-coordinator");
    }

    /**
     * Property 10: pangrova-connector-* must not depend on pangrova-worker or pangrova-coordinator.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void connectorsNeverDependOnWorkerOrCoordinator(
            @ForAll("connectorImportedModules") ModuleDep dep) {
        assertThat(dep.target())
                .as("Connector module '%s' must not depend on '%s'",
                        dep.source(), dep.target())
                .isNotIn("pangrova-worker", "pangrova-coordinator");
    }

    /**
     * Property 10: Cross-cutting providers (observability, governance, security, resilience)
     * must not depend on business execution modules.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void crossCuttingNeverDependsOnExecution(@ForAll("crossCuttingDeps") ModuleDep dep) {
        Set<String> executionModules = Set.of(
                "pangrova-coordinator", "pangrova-worker", "pangrova-parser",
                "pangrova-planner");
        assertThat(executionModules)
                .as("Cross-cutting module '%s' must not depend on execution module '%s'",
                        dep.source(), dep.target())
                .doesNotContain(dep.target());
    }

    /**
     * Property 10: Operations/resource modules must not depend on
     * pangrova-coordinator or pangrova-worker internals.
     *
     * <p><b>Validates: Requirements 3.5, 3.7, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 10: Module dependency direction correctness")
    void opsModulesNeverDependOnCoordinatorOrWorker(@ForAll("opsDeps") ModuleDep dep) {
        assertThat(dep.target())
                .as("Ops module '%s' must not depend on '%s'",
                        dep.source(), dep.target())
                .isNotIn("pangrova-coordinator", "pangrova-worker");
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> constrainedModules() {
        return Arbitraries.of(CONSTRAINED_MODULES);
    }

    @Provide
    Arbitrary<String> spiImportedModules() {
        Set<String> deps = collectImportedPangrovaModules("pangrova-spi");
        return deps.isEmpty()
                ? Arbitraries.just("pangrova-common")
                : Arbitraries.of(deps);
    }

    @Provide
    Arbitrary<String> protocolImportedModules() {
        Set<String> deps = collectImportedPangrovaModules("pangrova-protocol");
        return deps.isEmpty()
                ? Arbitraries.just("pangrova-common")
                : Arbitraries.of(deps);
    }

    @Provide
    Arbitrary<String> streamingImportedModules() {
        Set<String> deps = collectImportedPangrovaModules("pangrova-streaming");
        return deps.isEmpty()
                ? Arbitraries.just("pangrova-common")
                : Arbitraries.of(deps);
    }

    @Provide
    Arbitrary<ModuleDep> connectorImportedModules() {
        List<ModuleDep> deps = new ArrayList<>();
        for (String connector : List.of("pangrova-connector-iceberg", "pangrova-connector-paimon")) {
            for (String target : collectImportedPangrovaModules(connector)) {
                deps.add(new ModuleDep(connector, target));
            }
        }
        return deps.isEmpty()
                ? Arbitraries.just(new ModuleDep("pangrova-connector-iceberg", "pangrova-spi"))
                : Arbitraries.of(deps);
    }

    @Provide
    Arbitrary<ModuleDep> crossCuttingDeps() {
        List<ModuleDep> deps = new ArrayList<>();
        for (String module : List.of(
                "pangrova-observability", "pangrova-governance",
                "pangrova-security", "pangrova-resilience")) {
            for (String target : collectImportedPangrovaModules(module)) {
                deps.add(new ModuleDep(module, target));
            }
        }
        return deps.isEmpty()
                ? Arbitraries.just(new ModuleDep("pangrova-observability", "pangrova-common"))
                : Arbitraries.of(deps);
    }

    @Provide
    Arbitrary<ModuleDep> opsDeps() {
        List<ModuleDep> deps = new ArrayList<>();
        for (String module : List.of("pangrova-operations", "pangrova-resource")) {
            for (String target : collectImportedPangrovaModules(module)) {
                deps.add(new ModuleDep(module, target));
            }
        }
        return deps.isEmpty()
                ? Arbitraries.just(new ModuleDep("pangrova-operations", "pangrova-common"))
                : Arbitraries.of(deps);
    }

    // -----------------------------------------------------------------------
    // Scanning infrastructure
    // -----------------------------------------------------------------------

    private static Map<String, List<String>> computeAllViolations() {
        Map<String, List<String>> result = new HashMap<>();
        Path root = TestProjectPaths.findProjectRoot();

        for (String module : CONSTRAINED_MODULES) {
            Set<String> allowed = ALLOWED_DEPS.get(module);
            if (allowed == null) {
                continue;
            }
            Path srcDir = root.resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            List<String> violations = new ArrayList<>();
            scanModuleImports(srcDir, module, allowed, violations);
            if (!violations.isEmpty()) {
                result.put(module, List.copyOf(violations));
            }
        }
        return Map.copyOf(result);
    }

    private static Set<String> collectImportedPangrovaModules(String module) {
        Path root = TestProjectPaths.findProjectRoot();
        Path srcDir = root.resolve(module).resolve("src/main/java");
        if (!Files.isDirectory(srcDir)) {
            return Set.of();
        }
        java.util.Set<String> imported = new java.util.HashSet<>();
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(javaFile -> {
                        try {
                            for (String line : Files.readAllLines(javaFile)) {
                                String trimmed = line.trim();
                                if (trimmed.startsWith("package ")) {
                                    continue;
                                }
                                if (!trimmed.startsWith("import ")) {
                                    if (trimmed.startsWith("public ")
                                            || trimmed.startsWith("class ")
                                            || trimmed.startsWith("interface ")
                                            || trimmed.startsWith("enum ")
                                            || trimmed.startsWith("record ")
                                            || trimmed.startsWith("@")) {
                                        break;
                                    }
                                    continue;
                                }
                                Matcher m = IMPORT_PATTERN.matcher(trimmed);
                                if (m.matches()) {
                                    String importedFqn = m.group(1);
                                    String depModule = resolveModule(importedFqn);
                                    if (depModule != null && !depModule.equals(module)) {
                                        imported.add(depModule);
                                    }
                                }
                            }
                        } catch (IOException ignored) {
                            // skip unreadable files
                        }
                    });
        } catch (IOException ignored) {
            // skip if directory walk fails
        }
        return Set.copyOf(imported);
    }

    private static void scanModuleImports(
            Path srcDir, String module, Set<String> allowed, List<String> violations) {
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(javaFile -> {
                        try {
                            List<String> lines = Files.readAllLines(javaFile);
                            for (String line : lines) {
                                String trimmed = line.trim();
                                if (trimmed.startsWith("package ")) {
                                    continue;
                                }
                                if (!trimmed.startsWith("import ")) {
                                    if (trimmed.startsWith("public ")
                                            || trimmed.startsWith("class ")
                                            || trimmed.startsWith("interface ")
                                            || trimmed.startsWith("enum ")
                                            || trimmed.startsWith("record ")
                                            || trimmed.startsWith("@")) {
                                        break;
                                    }
                                    continue;
                                }
                                Matcher m = IMPORT_PATTERN.matcher(trimmed);
                                if (!m.matches()) {
                                    continue;
                                }
                                String importedFqn = m.group(1);
                                String depModule = resolveModule(importedFqn);
                                if (depModule == null || depModule.equals(module)) {
                                    continue;
                                }
                                if (!allowed.contains(depModule)) {
                                    String fileName = srcDir.relativize(javaFile).toString();
                                    violations.add(String.format(
                                            "%s imports %s (module: %s)",
                                            fileName, importedFqn, depModule));
                                }
                            }
                        } catch (IOException ignored) {
                            // skip unreadable files
                        }
                    });
        } catch (IOException ignored) {
            // skip if directory walk fails
        }
    }

    private static String resolveModule(String fqn) {
        for (PackageModuleMapping mapping : PACKAGE_TO_MODULE) {
            if (fqn.startsWith(mapping.packagePrefix())) {
                return mapping.moduleName();
            }
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Value types
    // -----------------------------------------------------------------------

    record PackageModuleMapping(String packagePrefix, String moduleName) {}

    record ModuleDep(String source, String target) {
        @Override
        public String toString() {
            return source + " -> " + target;
        }
    }
}
