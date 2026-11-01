package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying SPI interface implementation completeness.
 *
 * <p>Scans the SPI modules for key extension-point interfaces and verifies
 * that each has at least one non-NoOp implementation in the codebase, and that
 * the implementation is referenced in a Bootstrap class (Coordinator or Worker).</p>
 *
 * <p><b>Validates: Requirements 4.4</b></p>
 */
class SpiImplementationPropertyTest {

    /**
     * Key SPI interfaces that must have real (non-NoOp) implementations.
     * Each entry maps the simple interface name to its fully qualified name.
     * {@code indirectViaParent} indicates the interface is wired indirectly
     * through a parent SPI (e.g., ConnectorMetadata is returned by Connector).
     */
    private static final List<SpiContract> SPI_CONTRACTS = List.of(
            new SpiContract("Connector",
                    "org.pangrova.spi.connector.plugin.Connector",
                    Set.of("IcebergConnector", "PaimonConnector"),
                    null),
            new SpiContract("ConnectorFactory",
                    "org.pangrova.spi.connector.plugin.ConnectorFactory",
                    Set.of("IcebergConnectorFactory", "PaimonConnectorFactory"),
                    null),
            new SpiContract("ConnectorMetadata",
                    "org.pangrova.spi.connector.metadata.ConnectorMetadata",
                    Set.of("IcebergMetadata", "PaimonMetadata"),
                    "Connector"),
            new SpiContract("ConnectorSplitManager",
                    "org.pangrova.spi.connector.metadata.ConnectorSplitManager",
                    Set.of("IcebergSplitManager", "PaimonSplitManager"),
                    "Connector"),
            new SpiContract("ConnectorPageSource",
                    "org.pangrova.spi.connector.data.ConnectorPageSource",
                    Set.of("IcebergPageSource", "PaimonPageSource"),
                    "Connector"),
            new SpiContract("ConnectorPageSourceProvider",
                    "org.pangrova.spi.connector.data.ConnectorPageSourceProvider",
                    Set.of("IcebergPageSourceProvider", "PaimonPageSourceProvider"),
                    "Connector"),
            new SpiContract("ConnectorPageSinkProvider",
                    "org.pangrova.spi.connector.data.ConnectorPageSinkProvider",
                    Set.of("IcebergPageSinkProvider", "PaimonPageSinkProvider"),
                    "Connector"),
            new SpiContract("TaskExecutor",
                    "org.pangrova.worker.execution.TaskExecutor",
                    Set.of("NativeTaskExecutor"),
                    null),
            new SpiContract("MetricsProvider",
                    "org.pangrova.spi.observability.MetricsProvider",
                    Set.of("OtelMetricsProvider"),
                    null),
            new SpiContract("TracingProvider",
                    "org.pangrova.spi.observability.TracingProvider",
                    Set.of("OtelTracingProvider"),
                    null),
            new SpiContract("LoggingProvider",
                    "org.pangrova.spi.observability.LoggingProvider",
                    Set.of("OtelLoggingProvider"),
                    null),
            new SpiContract("AccessControl",
                    "org.pangrova.spi.security.AccessControl",
                    Set.of("DynamicRbacAccessControl"),
                    null),
            new SpiContract("CircuitBreakerPolicy",
                    "org.pangrova.common.resilience.CircuitBreakerPolicy",
                    Set.of("CircuitBreaker"),
                    null)
    );

    private static final List<String> SPI_NAMES =
            SPI_CONTRACTS.stream().map(SpiContract::simpleName).toList();

    /** NoOp class name patterns to exclude from "real implementation" checks. */
    private static final Pattern NOOP_PATTERN =
            Pattern.compile("(?i)noop|no_op|noOp|NoOp");

    /**
     * Bootstrap, wiring, and key orchestration files where SPI implementations
     * should be assembled or referenced. Includes bootstrap classes, module wiring,
     * plugin discovery factories, and key orchestration classes.
     */
    private static final List<String> BOOTSTRAP_FILES = List.of(
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/CoordinatorBootstrap.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/ModuleIntegrationBootstrap.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/ObservabilityBootstrap.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/WorkerBootstrap.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/CoordinatorBootstrapConfig.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/config/WorkerBootstrapConfig.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/metadata/PluginConnectorHandleFactory.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/metadata/MetadataBootstrap.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/PangrovaWorkerMain.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/PangrovaCoordinatorMain.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/PangrovaWorker.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/TaskManager.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/QueryManager.java",
            "pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/QueryDispatcher.java",
            "pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/phase/WorkerTaskBootstrap.java"
    );

    /** Pattern cache for implementation matching (must be initialized before SCAN_RESULTS). */
    private static final java.util.concurrent.ConcurrentHashMap<String, Pattern> IMPL_PATTERNS =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Lazily computed scan results: SPI simple name → ScanResult.
     */
    private static final Map<String, ScanResult> SCAN_RESULTS = scanAll();

    // -----------------------------------------------------------------------
    // Properties
    // -----------------------------------------------------------------------

    /**
     * Property 11: Every key SPI interface has at least one non-NoOp implementation.
     *
     * <p><b>Validates: Requirements 4.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 11: SPI interface implementation completeness")
    void everyKeySpiFaceHasRealImplementation(@ForAll("spiNames") String spiName) {
        ScanResult result = SCAN_RESULTS.get(spiName);
        assertThat(result)
                .as("No scan result for SPI '%s'", spiName)
                .isNotNull();
        assertThat(result.realImplementations())
                .as("SPI '%s' (%s) must have at least one non-NoOp implementation",
                        spiName, result.fqn())
                .isNotEmpty();
    }

    /**
     * Property 11: Every key SPI interface's real implementation is referenced
     * in a Bootstrap class (Coordinator or Worker).
     *
     * <p><b>Validates: Requirements 4.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 11: SPI interface implementation completeness")
    void everyKeySpiFaceIsWiredInBootstrap(@ForAll("spiNames") String spiName) {
        ScanResult result = SCAN_RESULTS.get(spiName);
        assertThat(result)
                .as("No scan result for SPI '%s'", spiName)
                .isNotNull();
        assertThat(result.wiredInBootstrap())
                .as("SPI '%s' must be wired in Bootstrap (referenced by impl or interface name)",
                        spiName)
                .isTrue();
    }

    /**
     * Property 11: The SPI interface file actually exists in the owning module.
     *
     * <p><b>Validates: Requirements 4.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 11: SPI interface implementation completeness")
    void spiInterfaceFileExists(@ForAll("spiNames") String spiName) {
        ScanResult result = SCAN_RESULTS.get(spiName);
        assertThat(result)
                .as("No scan result for SPI '%s'", spiName)
                .isNotNull();
        assertThat(result.interfaceFileExists())
                .as("SPI interface file for '%s' must exist under a module src/main/java", spiName)
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> spiNames() {
        return Arbitraries.of(SPI_NAMES);
    }

    // -----------------------------------------------------------------------
    // Scanning infrastructure
    // -----------------------------------------------------------------------

    private static Map<String, ScanResult> scanAll() {
        Path root = TestProjectPaths.findProjectRoot();
        java.util.Map<String, ScanResult> results = new java.util.HashMap<>();

        String bootstrapContent = readBootstrapFiles(root);

        for (SpiContract contract : SPI_CONTRACTS) {
            boolean interfaceExists = checkInterfaceExists(root, contract.fqn());
            List<String> realImpls = findRealImplementations(root, contract);
            boolean wired = checkWiredInBootstrap(bootstrapContent, contract, realImpls);

            results.put(contract.simpleName(), new ScanResult(
                    contract.fqn(),
                    interfaceExists,
                    List.copyOf(realImpls),
                    wired));
        }
        return Map.copyOf(results);
    }

    private static boolean checkInterfaceExists(Path root, String fqn) {
        String javaPath = fqn.replace('.', '/') + ".java";
        try (Stream<Path> modules = Files.walk(root, 2)) {
            return modules
                    .filter(Files::isDirectory)
                    .map(module -> module.resolve("src/main/java"))
                    .filter(Files::isDirectory)
                    .anyMatch(sourceRoot -> Files.isRegularFile(sourceRoot.resolve(javaPath)));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot scan module sources under " + root, e);
        }
    }

    /**
     * Scans all module source directories for classes implementing the SPI interface.
     * Excludes NoOp implementations and test classes.
     */
    private static List<String> findRealImplementations(Path root, SpiContract contract) {
        List<String> implementations = new ArrayList<>();
        String implementsClause = "implements " + contract.simpleName();
        String implementsClauseFqn = "implements " + contract.fqn();

        // Scan all module src/main/java directories (including nested aggregator children)
        try (Stream<Path> moduleDirs = Files.list(root)) {
            moduleDirs
                    .filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("pangrova-"))
                    .filter(p -> !p.getFileName().toString().equals("pangrova-testing"))
                    .forEach(moduleDir -> scanModuleDir(
                            moduleDir, implementsClause, implementsClauseFqn,
                            contract, implementations));
        } catch (IOException ignored) {
            // skip if root listing fails
        }
        return implementations;
    }

    private static void scanModuleDir(Path moduleDir, String implementsClause,
            String implementsClauseFqn, SpiContract contract, List<String> implementations) {
        Path srcDir = moduleDir.resolve("src/main/java");
        if (Files.isDirectory(srcDir)) {
            scanForImplementations(srcDir, implementsClause, implementsClauseFqn,
                    contract, implementations);
        }
        // Also scan nested sub-modules (e.g., pangrova-connector/pangrova-connector-iceberg/)
        try (Stream<Path> children = Files.list(moduleDir)) {
            children.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("pangrova-"))
                    .forEach(child -> {
                        Path childSrc = child.resolve("src/main/java");
                        if (Files.isDirectory(childSrc)) {
                            scanForImplementations(childSrc, implementsClause,
                                    implementsClauseFqn, contract, implementations);
                        }
                    });
        } catch (IOException ignored) {
            // skip if listing fails
        }
    }

    private static void scanForImplementations(
            Path srcDir, String implementsClause, String implementsClauseFqn,
            SpiContract contract, List<String> implementations) {
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(javaFile -> {
                        try {
                            String content = Files.readString(javaFile);
                            if (containsImplementsClause(content, implementsClause, implementsClauseFqn)) {
                                String className = extractClassName(javaFile);
                                if (className != null
                                        && !NOOP_PATTERN.matcher(className).find()
                                        && !isKnownNoOp(className, contract)) {
                                    implementations.add(className);
                                }
                            }
                        } catch (IOException ignored) {
                            // skip unreadable files
                        }
                    });
        } catch (IOException ignored) {
            // skip if walk fails
        }
    }

    private static boolean containsImplementsClause(
            String content, String implementsClause, String implementsClauseFqn) {
        return matchesWithWordBoundary(content, implementsClause)
                || matchesWithWordBoundary(content, implementsClauseFqn);
    }

    private static boolean matchesWithWordBoundary(String content, String clause) {
        Pattern pattern = IMPL_PATTERNS.computeIfAbsent(clause,
                k -> Pattern.compile("\\b" + Pattern.quote(k) + "\\b"));
        return pattern.matcher(content).find();
    }

    /**
     * Extracts the simple class name from a Java source file path.
     */
    private static String extractClassName(Path javaFile) {
        String fileName = javaFile.getFileName().toString();
        if (fileName.endsWith(".java")) {
            return fileName.substring(0, fileName.length() - 5);
        }
        return null;
    }

    /**
     * Checks if a class name is a known NoOp implementation that should be excluded.
     */
    private static boolean isKnownNoOp(String className, SpiContract contract) {
        String lower = className.toLowerCase();
        return lower.startsWith("noop")
                || lower.contains("noop")
                || lower.contains("no_op")
                || lower.contains("stub")
                || lower.contains("fake")
                || lower.contains("dummy");
    }

    /**
     * Checks if the SPI interface or any of its real implementations are referenced
     * in Bootstrap files. For interfaces wired indirectly through a parent SPI
     * (e.g., ConnectorMetadata via Connector), checks the parent's wiring instead.
     */
    private static boolean checkWiredInBootstrap(
            String bootstrapContent, SpiContract contract, List<String> realImpls) {
        // If this SPI is wired indirectly through a parent, check the parent
        if (contract.indirectViaParent() != null) {
            return bootstrapContent.contains(contract.indirectViaParent());
        }
        // Check if the interface name itself is referenced in bootstrap
        if (bootstrapContent.contains(contract.simpleName())) {
            return true;
        }
        // Check if any known expected implementation is referenced
        for (String expected : contract.expectedImpls()) {
            if (bootstrapContent.contains(expected)) {
                return true;
            }
        }
        // Check if any discovered real implementation is referenced
        for (String impl : realImpls) {
            if (bootstrapContent.contains(impl)) {
                return true;
            }
        }
        return false;
    }

    private static String readBootstrapFiles(Path root) {
        StringBuilder sb = new StringBuilder();
        for (String bootstrapFile : BOOTSTRAP_FILES) {
            Path path = root.resolve(bootstrapFile);
            try {
                sb.append(Files.readString(path));
                sb.append('\n');
            } catch (IOException failure) {
                throw new UncheckedIOException("Cannot read required bootstrap source " + path, failure);
            }
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // Value types
    // -----------------------------------------------------------------------

    record SpiContract(String simpleName, String fqn, Set<String> expectedImpls,
                       String indirectViaParent) {}

    record ScanResult(
            String fqn,
            boolean interfaceFileExists,
            List<String> realImplementations,
            boolean wiredInBootstrap) {}
}
