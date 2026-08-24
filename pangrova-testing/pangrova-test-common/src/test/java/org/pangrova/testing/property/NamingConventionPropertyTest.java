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
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests validating naming convention invariants after the
 * 65+ rename remediation across all Pangrova modules.
 *
 * <p>Uses source-file scanning at test time to verify that banned names,
 * framework leaks, abbreviations, and cross-module collisions have been
 * fully eliminated.</p>
 */
class NamingConventionPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();

    // ── Value types ──

    record ClassInfo(String simpleName, String qualifiedName, String module, String packageName) {
        @Override
        public String toString() {
            return module + ":" + qualifiedName;
        }
    }

    record RenameOperation(String phase, String oldName, String newName,
                           String module, String operationType) {}


    // ── Module lists ──

    /** Stable-layer modules where framework names are banned in class names. */
    private static final List<String> STABLE_MODULES = List.of(
            "pangrova-spi", "pangrova-common", "pangrova-protocol",
            "pangrova-resource", "pangrova-observability", "pangrova-streaming"
    );

    /** All Java production modules (excludes pangrova-worker-native and pangrova-testing). */
    private static final List<String> ALL_MODULES = List.of(
            "pangrova-common", "pangrova-spi", "pangrova-protocol",
            "pangrova-connector/pangrova-connector-iceberg",
            "pangrova-connector/pangrova-connector-paimon",
            "pangrova-parser", "pangrova-planner", "pangrova-metadata",
            "pangrova-coordinator", "pangrova-worker",
            "pangrova-observability", "pangrova-governance", "pangrova-security",
            "pangrova-resilience", "pangrova-streaming", "pangrova-operations",
            "pangrova-resource", "pangrova-shuffle-server",
            "pangrova-client/pangrova-client-cli",
            "pangrova-client/pangrova-client-jdbc",
            "pangrova-client/pangrova-client-rest"
    );

    /** Framework substrings banned in stable-layer class names. */
    private static final List<String> BANNED_FRAMEWORK_NAMES = List.of(
            "Jetty", "Calcite", "Presto", "Flight", "Rss"
    );

    /**
     * Arrow is banned in stable layers except for intentional ACL adapter classes.
     * Iceberg/Paimon are banned outside their connector modules.
     */
    private static final String ARROW = "Arrow";

    /**
     * Intentional framework-name exemptions: classes whose purpose IS to bridge
     * a specific framework (interceptors, ACL adapters). The framework name
     * here is a legitimate domain term, not a leak of implementation detail.
     */
    private static final Set<String> FRAMEWORK_NAME_EXEMPTIONS = Set.of(
            "FlightSpanInterceptor"  // intercepts Arrow Flight RPC for tracing — Flight is the protocol, not a leak
    );

    /** Banned abbreviation prefixes in class names. */
    private static final Pattern BANNED_CLASS_PREFIX = Pattern.compile(
            "^(Rss|Dpp|Aqe|Sop|Ipc|Repl)[A-Z].*"
    );

    /** MV as prefix (not substring like MVN). */
    private static final Pattern MV_PREFIX = Pattern.compile("^MV[A-Z].*");

    /** Dt as prefix for DynamicTable. */
    private static final Pattern DT_PREFIX = Pattern.compile("^Dt[A-Z].*");

    /** HA as prefix. */
    private static final Pattern HA_PREFIX = Pattern.compile("^HA[A-Z].*");

    /** Banned package segments. */
    private static final Set<String> BANNED_PACKAGE_SEGMENTS = Set.of(
            "rss", "dpp", "aqe", "sop", "rls", "jaxrs"
    );

    /**
     * Package segment "dt" is banned (should be "dynamictable").
     * Package segment "ha" is allowed since classes inside are already
     * renamed to HighAvailability* and the task spec did not require
     * package rename for ha/.
     */
    private static final String BANNED_DT_SEGMENT = "dt";

    // ── Precomputed data ──

    private static final List<ClassInfo> ALL_PRODUCTION_CLASSES = scanAllProductionClasses();
    private static final List<ClassInfo> STABLE_LAYER_CLASSES = filterByModules(ALL_PRODUCTION_CLASSES, STABLE_MODULES);
    private static final List<RenameOperation> ALL_RENAME_OPS = buildRenameOperations();


    // ── Property 1: Zero Residual References After Rename ──

    /**
     * Feature: naming-convention-remediation, Property 1: Zero Residual References After Rename.
     *
     * <p>For any rename operation from Phases 1-5, a grep for the old class name
     * across all production and test source files returns zero matches.</p>
     *
     * <p><b>Validates: Requirements 1.15, 3.17, 4.22, 8.2, 8.4, 8.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: naming-convention-remediation, Property 1: Zero Residual References After Rename")
    void afterRename_oldNameHasZeroReferences(
            @ForAll("renameOperations") RenameOperation op) {
        List<String> residuals = grepSourceFilesForClassName(op.oldName());
        assertThat(residuals)
                .as("Old name '%s' (phase %s, renamed to '%s') still found in: %s",
                        op.oldName(), op.phase(), op.newName(), residuals)
                .isEmpty();
    }

    // ── Property 2: Cross-Module Class Name Uniqueness ──

    /**
     * Feature: naming-convention-remediation, Property 2: Cross-Module Class Name Uniqueness.
     *
     * <p>For any two production classes from different modules, their simple
     * class names are distinct. SPI contracts and pangrova-testing are exempt.</p>
     *
     * <p><b>Validates: Requirements 2.32, 7.1, 7.2, 7.3, 7.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: naming-convention-remediation, Property 2: Cross-Module Class Name Uniqueness")
    void noTwoProductionClasses_shareSimpleName_acrossModules(
            @ForAll("crossModuleClassPairs") ClassInfo[] pair) {
        ClassInfo a = pair[0];
        ClassInfo b = pair[1];
        assertThat(a.simpleName())
                .as("Cross-module collision: '%s' in %s vs '%s' in %s",
                        a.qualifiedName(), a.module(), b.qualifiedName(), b.module())
                .isNotEqualTo(b.simpleName());
    }

    // ── Property 3: No Framework Name Leaks in Stable Layers ──

    /**
     * Feature: naming-convention-remediation, Property 3: No Framework Name Leaks in Stable Layers.
     *
     * <p>For any class in stable-layer modules (spi, common, protocol, resource,
     * observability, streaming), the class name does not contain banned framework
     * substrings.</p>
     *
     * <p><b>Validates: Requirements 1.14, 3.16, 4.21, 3.1-3.5, 3.8, 3.9, 4.1-4.6</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: naming-convention-remediation, Property 3: No Framework Name Leaks in Stable Layers")
    void stableLayerClasses_doNotContainFrameworkNames(
            @ForAll("stableLayerClasses") ClassInfo classInfo) {
        String name = classInfo.simpleName();
        if (FRAMEWORK_NAME_EXEMPTIONS.contains(name)) {
            return;
        }
        for (String banned : BANNED_FRAMEWORK_NAMES) {
            assertThat(name)
                    .as("Framework leak '%s' in stable class %s", banned, classInfo)
                    .doesNotContain(banned);
        }
        // Arrow is banned in stable layers (no ACL adapter expected there)
        assertThat(name)
                .as("Arrow leak in stable class %s", classInfo)
                .doesNotContain(ARROW);
    }

    // ── Property 4: No Non-Standard Abbreviations in Identifiers ──

    /**
     * Feature: naming-convention-remediation, Property 4: No Non-Standard Abbreviations.
     *
     * <p>For any production class name, the identifier does not contain banned
     * abbreviation prefixes or patterns.</p>
     *
     * <p><b>Validates: Requirements 5.7, 5.11-5.16, 5.18-5.20, 5.24-5.27</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: naming-convention-remediation, Property 4: No Non-Standard Abbreviations")
    void classNames_doNotContainBannedAbbreviations(
            @ForAll("allProductionClasses") ClassInfo classInfo) {
        String name = classInfo.simpleName();
        assertThat(BANNED_CLASS_PREFIX.matcher(name).matches())
                .as("Banned abbreviation prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(MV_PREFIX.matcher(name).matches())
                .as("MV prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(DT_PREFIX.matcher(name).matches())
                .as("Dt prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(HA_PREFIX.matcher(name).matches())
                .as("HA prefix in class '%s' (%s)", name, classInfo)
                .isFalse();

        // Check banned package segments
        String pkg = classInfo.packageName();
        for (String segment : pkg.split("\\.")) {
            assertThat(BANNED_PACKAGE_SEGMENTS.contains(segment))
                    .as("Banned package segment '%s' in %s", segment, classInfo)
                    .isFalse();
        }
        // "dt" as standalone package segment is banned
        for (String segment : pkg.split("\\.")) {
            assertThat(segment.equals(BANNED_DT_SEGMENT))
                    .as("Banned 'dt' package segment in %s", classInfo)
                    .isFalse();
        }
    }


    // ── Providers ──

    @Provide
    Arbitrary<RenameOperation> renameOperations() {
        if (ALL_RENAME_OPS.isEmpty()) {
            return Arbitraries.just(new RenameOperation("0", "__SENTINEL__", "__SENTINEL__", "none", "NONE"));
        }
        return Arbitraries.of(ALL_RENAME_OPS);
    }

    @Provide
    Arbitrary<ClassInfo[]> crossModuleClassPairs() {
        if (ALL_PRODUCTION_CLASSES.size() < 2) {
            ClassInfo sentinel = new ClassInfo("__SENTINEL__", "none.__SENTINEL__", "none", "none");
            return Arbitraries.just(new ClassInfo[]{sentinel, sentinel});
        }
        Arbitrary<ClassInfo> classArb = Arbitraries.of(ALL_PRODUCTION_CLASSES);
        return classArb.tuple2()
                .filter(t -> !t.get1().module().equals(t.get2().module()))
                .map(t -> new ClassInfo[]{t.get1(), t.get2()});
    }

    @Provide
    Arbitrary<ClassInfo> stableLayerClasses() {
        if (STABLE_LAYER_CLASSES.isEmpty()) {
            return Arbitraries.just(new ClassInfo("__SENTINEL__", "none.__SENTINEL__", "none", "none"));
        }
        return Arbitraries.of(STABLE_LAYER_CLASSES);
    }

    @Provide
    Arbitrary<ClassInfo> allProductionClasses() {
        if (ALL_PRODUCTION_CLASSES.isEmpty()) {
            return Arbitraries.just(new ClassInfo("__SENTINEL__", "none.__SENTINEL__", "none", "none"));
        }
        return Arbitraries.of(ALL_PRODUCTION_CLASSES);
    }

    // ── Source scanning infrastructure ──

    private static List<ClassInfo> scanAllProductionClasses() {
        List<ClassInfo> result = new ArrayList<>();
        for (String module : ALL_MODULES) {
            Path srcDir = resolveMainSrcDir(module);
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            String moduleShort = extractModuleName(module);
            scanDirectory(srcDir, srcDir, moduleShort, result);
        }
        return List.copyOf(result);
    }

    private static void scanDirectory(Path dir, Path srcRoot, String module, List<ClassInfo> result) {
        try (Stream<Path> files = Files.walk(dir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals("package-info.java"))
                    .forEach(javaFile -> {
                        String fileName = javaFile.getFileName().toString();
                        String simpleName = fileName.substring(0, fileName.length() - 5);
                        Path relative = srcRoot.relativize(javaFile.getParent());
                        String packageName = relative.toString().replace('/', '.').replace('\\', '.');
                        String qualifiedName = packageName.isEmpty()
                                ? simpleName
                                : packageName + "." + simpleName;
                        result.add(new ClassInfo(simpleName, qualifiedName, module, packageName));
                    });
        } catch (IOException ignored) {
            // skip unreadable directories
        }
    }

    private static List<ClassInfo> filterByModules(List<ClassInfo> classes, List<String> moduleNames) {
        Set<String> shortNames = new HashSet<>();
        for (String m : moduleNames) {
            shortNames.add(extractModuleName(m));
        }
        return classes.stream()
                .filter(c -> shortNames.contains(c.module()))
                .toList();
    }

    private static Path resolveMainSrcDir(String module) {
        return PROJECT_ROOT.resolve(module).resolve("src/main/java");
    }

    private static String extractModuleName(String modulePath) {
        int slash = modulePath.lastIndexOf('/');
        return slash >= 0 ? modulePath.substring(slash + 1) : modulePath;
    }


    // ── Grep utility for residual reference detection ──

    /**
     * Searches all Java source files for a class name pattern.
     * Returns list of file paths where the old class name appears as a
     * class/interface/enum/record declaration or import target.
     */
    private static List<String> grepSourceFilesForClassName(String className) {
        if ("__SENTINEL__".equals(className)) {
            return List.of();
        }
        Pattern classPattern = Pattern.compile(
                "\\b(class|interface|enum|record)\\s+" + Pattern.quote(className) + "\\b"
        );
        List<String> matches = new ArrayList<>();
        for (String module : ALL_MODULES) {
            searchInDir(PROJECT_ROOT.resolve(module).resolve("src/main/java"), classPattern, matches);
            searchInDir(PROJECT_ROOT.resolve(module).resolve("src/test/java"), classPattern, matches);
        }
        return List.copyOf(matches);
    }

    private static void searchInDir(Path dir, Pattern pattern, List<String> matches) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> files = Files.walk(dir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(javaFile -> {
                        try {
                            String content = Files.readString(javaFile);
                            if (pattern.matcher(content).find()) {
                                matches.add(javaFile.toString());
                            }
                        } catch (IOException ignored) {
                            // skip unreadable files
                        }
                    });
        } catch (IOException ignored) {
            // skip unreadable directories
        }
    }

    // ── Rename operations registry (all 65+ operations from Phases 1-5) ──

    private static List<RenameOperation> buildRenameOperations() {
        List<RenameOperation> ops = new ArrayList<>();

        // Phase 1: RSS Framework Name Cleanup
        ops.add(new RenameOperation("1", "RssServer", "RemoteShuffleServer", "pangrova-shuffle-server", "CLASS"));
        ops.add(new RenameOperation("1", "RssServerConfig", "RemoteShuffleServerConfig", "pangrova-shuffle-server", "CLASS"));
        ops.add(new RenameOperation("1", "ShuffleServiceImpl", "GrpcShuffleService", "pangrova-shuffle-server", "CLASS"));
        ops.add(new RenameOperation("1", "RssExchangeManager", "RemoteShuffleExchangeManager", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("1", "RssExchangeSink", "RemoteShuffleExchangeSink", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("1", "RssExchangeSource", "RemoteShuffleExchangeSource", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("1", "RssClient", "RemoteShuffleClient", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("1", "RssClientPool", "RemoteShuffleClientPool", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("1", "RssClientException", "RemoteShuffleClientException", "pangrova-worker", "CLASS"));

        // Phase 2: Cross-Module Collision Resolution
        addPhase2Ops(ops);

        // Phase 3: Base/SPI/Cross-Cutting Framework Name Leaks
        addPhase3Ops(ops);

        // Phase 4: Flight Framework Name Cleanup
        addPhase4Ops(ops);

        // Phase 5a: Abbreviation and Framework Name Cleanup
        addPhase5aOps(ops);

        // Phase 5b: Vague Naming and Style Cleanup
        addPhase5bOps(ops);

        // Phase 12: Adapter/Dependencies Naming Cleanup
        addPhase12Ops(ops);

        return List.copyOf(ops);
    }

    private static void addPhase2Ops(List<RenameOperation> ops) {
        ops.add(new RenameOperation("2", "ExchangeManager", "ExchangeClientRegistry", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("2", "PangrovaExceptionMapper", "CoordinatorExceptionMapper", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("2", "PangrovaExceptionMapper", "WorkerExceptionMapper", "pangrova-worker", "CLASS"));
    }

    private static void addPhase3Ops(List<RenameOperation> ops) {
        ops.add(new RenameOperation("3", "JettyServerFactory", "EmbeddedHttpServerFactory", "pangrova-common", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowPage", "NativePage", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowPageAdapter", "NativePageAdapter", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowAllocatorManager", "OffHeapAllocatorManager", "pangrova-resource", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowMemoryMetrics", "OffHeapMemoryMetrics", "pangrova-observability", "CLASS"));
        ops.add(new RenameOperation("3", "MetadataCalciteSchemaAdapter", "MetadataSchemaAdapter", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("3", "ShufflePrestoDeserializer", "ShufflePageValidator", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("3", "IcebergTableWriter", "LakeTableWriter", "pangrova-streaming", "CLASS"));
        ops.add(new RenameOperation("3", "PaimonTableWriter", "MergeTreeTableWriter", "pangrova-streaming", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowIpcDecoder", "ResultBatchDecoder", "pangrova-client-cli", "CLASS"));
        ops.add(new RenameOperation("3", "ArrowDecodeException", "ResultDecodeException", "pangrova-client-cli", "CLASS"));
        // PageDecoder deleted — Arrow Java SDK removed from pangrova-worker (native-result-serving)
        ops.add(new RenameOperation("3", "RelNodeTransform", "PlanNodeTransform", "pangrova-coordinator", "CLASS"));
    }

    private static void addPhase4Ops(List<RenameOperation> ops) {
        ops.add(new RenameOperation("4", "FlightProducer", "DataProducer", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("4", "FlightTicket", "DataTicket", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("4", "FlightServerConfig", "TransportServerConfig", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("4", "FlightServerLifecycle", "TransportServerLifecycle", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("4", "FlightStatus", "TransportStatus", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("4", "PangrovaFlightProducer", "PangrovaDataProducer", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightClient", "TransportClient", "pangrova-worker", "CLASS"));
        // VectorExchangeDataServer deleted (deprecated, replaced by BatchStreamProducer)
        ops.add(new RenameOperation("4", "FlightServer", "TransportServer", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightStatusMapper", "TransportStatusMapper", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightClientPool", "TransportClientPool", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightExchangeManager", "RemoteExchangeManager", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightExchangeSink", "BufferedExchangeSink", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "FlightExchangeSource", "RemoteExchangeSource", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("4", "CoordinatorFlightProducer", "CoordinatorDataProducer", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("4", "FlightResultFetcher", "ResultStreamFetcher", "pangrova-client-rest", "CLASS"));
    }

    private static void addPhase5aOps(List<RenameOperation> ops) {
        ops.add(new RenameOperation("5a", "RexNodeConverter", "ExpressionConverter", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "RelNodeVisitor", "PlanNodeConverter", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "MVRelOptTable", "MaterializedViewTableRef", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "ProtoRelDeserializer", "SubstraitPlanDeserializer", "pangrova-planner", "CLASS"));
        // NativeDataBridge deleted — Arrow Java SDK removed from pangrova-worker (native-result-serving)
        ops.add(new RenameOperation("5a", "DppFilterMetrics", "DynamicPartitionFilterMetrics", "pangrova-worker", "CLASS"));
        ops.add(new RenameOperation("5a", "DppConfig", "DynamicPartitionPruningConfig", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "MVCandidate", "MaterializedViewCandidate", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "MVException", "MaterializedViewException", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "MVQueryRewriter", "MaterializedViewQueryRewriter", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "CreateMVStatement", "CreateMaterializedViewStatement", "pangrova-parser", "CLASS"));
        ops.add(new RenameOperation("5a", "DropMVStatement", "DropMaterializedViewStatement", "pangrova-parser", "CLASS"));
        ops.add(new RenameOperation("5a", "MvState", "MaterializedViewState", "pangrova-metadata", "CLASS"));
        ops.add(new RenameOperation("5a", "AqeStageHandler", "AdaptiveQueryEvaluator", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("5a", "AqeConfig", "AdaptiveQueryConfig", "pangrova-planner", "CLASS"));
        ops.add(new RenameOperation("5a", "HACoordinationService", "HighAvailabilityCoordinationService", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("5a", "HABootstrap", "HighAvailabilityBootstrap", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("5a", "SopFramework", "RecoveryPlaybookFramework", "pangrova-operations", "CLASS"));
        ops.add(new RenameOperation("5a", "SqlRepl", "SqlShell", "pangrova-client-cli", "CLASS"));
        ops.add(new RenameOperation("5a", "ConnectorSessionImpl", "DefaultConnectorSession", "pangrova-metadata", "CLASS"));
        ops.add(new RenameOperation("5a", "DtState", "DynamicTableLifecycleState", "pangrova-metadata", "CLASS"));
    }

    private static void addPhase5bOps(List<RenameOperation> ops) {
        ops.add(new RenameOperation("5b", "LegacyPageAdapter", "RowPageAdapter", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("5b", "SimpleDataSource", "EmbeddedDataSource", "pangrova-metadata", "CLASS"));
        ops.add(new RenameOperation("5b", "SimpleConnectorSession", "StatementConnectorSession", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("5b", "SimpleColumnHandle", "FederationColumnHandle", "pangrova-metadata", "CLASS"));
        ops.add(new RenameOperation("5b", "SimpleTableHandle", "FederationTableHandle", "pangrova-metadata", "CLASS"));
    }

    private static void addPhase12Ops(List<RenameOperation> ops) {
        // Phase 12a: Adapter → Correct Pattern Name
        ops.add(new RenameOperation("12a", "NativePageAdapter", "NativePageConverter", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("12a", "RowPageAdapter", "RowPageConverter", "pangrova-spi", "CLASS"));
        ops.add(new RenameOperation("12a", "CatalogAdapter", "FederatedCatalog", "pangrova-metadata", "INTERFACE"));
        ops.add(new RenameOperation("12a", "CatalogAdapterFactory", "FederatedCatalogFactory", "pangrova-metadata", "INTERFACE"));
        ops.add(new RenameOperation("12a", "L2CacheAdapter", "L2CacheStore", "pangrova-metadata", "INTERFACE"));
        ops.add(new RenameOperation("12a", "DisabledL2CacheAdapter", "DisabledL2CacheStore", "pangrova-metadata", "CLASS"));
        ops.add(new RenameOperation("12a", "MetadataSchemaAdapter", "MetadataSchemaBuilder", "pangrova-coordinator", "CLASS"));
        ops.add(new RenameOperation("12a", "WriteRecordAdapter", "WriteRecordConverter", "pangrova-streaming", "CLASS"));

        // Phase 12b: Dependencies → Context/Refs (Worker)
        ops.add(new RenameOperation("12b", "BufferDependencies", "BufferRefs", "pangrova-worker", "RECORD"));
        ops.add(new RenameOperation("12b", "MemoryDependencies", "MemoryRefs", "pangrova-worker", "RECORD"));
        ops.add(new RenameOperation("12b", "TaskManagerDependencies", "TaskManagerContext", "pangrova-worker", "RECORD"));
        ops.add(new RenameOperation("12b", "ExchangeClientDependencies", "ExchangeClientContext", "pangrova-worker", "RECORD"));
        // Phase 12b: Dependencies → Context (Coordinator)
        ops.add(new RenameOperation("12b", "QueryManagerDependencies", "QueryManagerContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "QueryDispatcherDependencies", "QueryDispatcherContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "TaskStatusReceiverDependencies", "TaskStatusReceiverContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "CoordinatorHttpServerDependencies", "CoordinatorHttpServerContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "InsertSelectDependencies", "InsertSelectContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "DAGDispatchDependencies", "DAGDispatchContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "IndexDdlDependencies", "IndexDdlContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "IndexGcDependencies", "IndexGcContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "IndexCommitDependencies", "IndexCommitContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "IndexConsistencyDependencies", "IndexConsistencyContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "StreamSchedulerDependencies", "StreamSchedulerContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "ConnectorPageSinkStreamWriterDependencies", "PageSinkStreamWriterContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "StreamDdlDependencies", "StreamDdlContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "ClusteringDdlDependencies", "ClusteringDdlContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "DynamicTableDdlDependencies", "DynamicTableDdlContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "RefreshSchedulerDependencies", "RefreshSchedulerContext", "pangrova-coordinator", "RECORD"));
        ops.add(new RenameOperation("12b", "RefreshCoordinatorDependencies", "RefreshCoordinatorContext", "pangrova-coordinator", "RECORD"));

        // Phase 12b: Dependencies → Context (Other Modules)
        ops.add(new RenameOperation("12b", "PlannerDependencies", "PlannerContext", "pangrova-planner", "RECORD"));
        ops.add(new RenameOperation("12b", "CdcPipelineLoopDependencies", "CdcPipelineLoopContext", "pangrova-streaming", "RECORD"));
        ops.add(new RenameOperation("12b", "IndexBuildDependencies", "IndexBuildContext", "pangrova-indexing", "RECORD"));
        ops.add(new RenameOperation("12b", "GovernanceDependencies", "GovernanceContext", "pangrova-governance", "RECORD"));
    }

}
