package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

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
 * Property 32: State Enum Authoritative Uniqueness.
 *
 * <p>Verifies that SPI-level state enums (QueryState, TaskStatus.TaskState, TransportStatus,
 * HealthStatus) each have exactly one authoritative definition in the stable upstream
 * contract modules (the {@code -api}/{@code -spi} artifacts declared in
 * {@code .pangrova/modules.yaml}, plus {@code pangrova-protocol}).</p>
 *
 * <p>Internal state machines (QueryStateMachine.State, worker.task.state.TaskState,
 * TaskStateMachine.State, StageStateMachine.State) are allowed as module-internal
 * representations, provided they do not duplicate the SPI enum name in a different
 * stable upstream module.</p>
 *
 * <p><b>Validates: Requirements 5.2, 8.1</b></p>
 */
class StateEnumUniquenessPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();

    /**
     * SPI-level state enum simple names that must have exactly one definition
     * in stable upstream contract modules.
     */
    private static final Set<String> SPI_AUTHORITATIVE_ENUMS = Set.of(
            "QueryState",
            "SnapshotState",
            "TransportStatus"
    );

    /**
     * Stable upstream contract modules where SPI state enums must be defined.
     * Mirrors the {@code -api}/{@code -spi} artifacts in {@code .pangrova/modules.yaml}.
     */
    private static final Set<String> STABLE_UPSTREAM_MODULES = Set.of(
            "pangrova-protocol",
            "pangrova-connector-spi",
            "pangrova-observability-api",
            "pangrova-indexing-api",
            "pangrova-ai-api"
    );

    /**
     * Known internal state enums that are intentionally module-scoped.
     * These are allowed because they serve a different lifecycle granularity
     * than the SPI enums and have proper mapping layers.
     */
    private static final Set<String> KNOWN_INTERNAL_STATE_ENUMS = Set.of(
            "pangrova-coordination-core:QueryStateMachine.State",
            "pangrova-streaming:ConnectorPageSinkCdcWriter.PublicationState",
            "pangrova-coordination-core:TaskStateMachine.State",
            "pangrova-coordination-core:StageStateMachine.State",
            "pangrova-scheduler:HealthState",
            "pangrova-worker-core:TaskState",
            // One-shot local assembly admission/cleanup phases, not distributed Worker health or task state.
            "pangrova-worker-core:WorkerBootstrapLifecycle.State",
            "pangrova-worker-core:ExchangeClient.State",
            "pangrova-worker-core:RemoteShuffleProducerStream.State",
            "pangrova-operations:DdlJob.DdlJobState",
            "pangrova-metadata:CatalogState",
            "pangrova-table:TableControlState",  // LINKED/MANAGED control-plane lifecycle
            "pangrova-table:MaintenanceRunState",  // control-plane maintenance run lifecycle
            "pangrova-streaming:CdcStreamState",
            "pangrova-streaming:WatermarkState",
            "pangrova-security:TenantStatus",
            "pangrova-coordination-core:CircuitBreaker.State",
            "pangrova-coordination-core:SplitRetryManager.SplitState",
            "pangrova-connector-spi:HealthCheckResult.HealthStatus",
            "pangrova-protocol:TaskStatus.TaskState",
            "pangrova-observability-api:StatusCode",
            "pangrova-planner:ValidationStatus",
            "pangrova-coordination-core:SchedulerCancelResult.Status",
            "pangrova-security:AuthenticatorHealthStatus",
            "pangrova-worker-core:TaskCancelResult.Status",
            "pangrova-streaming:StreamLifecycleState",
            "pangrova-semantic:SemanticViewValidationState",
            "pangrova-ai-control:EvaluationRunState",
            "pangrova-connector-spi:ClusteringState",
            "pangrova-indexing-api:IndexState",
            "pangrova-common:StateStoreType",  // moved from pangrova-config in cbe99e06 module merge
            "pangrova-config:StateStoreType",  // legacy copy retained for StateStoreFactory/CoordinatorOptions
            "pangrova-streaming:CdcRegistration.CdcStreamState",
            "pangrova-coordination-core:StatementCategory",
            "pangrova-coordination-core:ReclusterJob.State",  // data recluster lifecycle
            "pangrova-parser:StatementType",  // false positive — discriminator enum, "StatementType" contains "State" substring
            "pangrova-streaming:CdcCommitState",  // CDC two-phase commit coordinator (Req 10.6)
            "pangrova-operations:JfrState",  // JFR profiling lifecycle
            "pangrova-coordination-core:SettableDynamicFilter.State",  // adaptive dynamic filter state
            "pangrova-indexing-core:IndexFileReadResult.Status",  // index file read result status
            "pangrova-connector-spi:WriteCommitEntry.State",  // distributed write commit lifecycle
            "pangrova-metadata:SemanticViewValidationState",
            "pangrova-metadata:EvaluationRunState",
            "pangrova-generation-core:GenerationJobState",
            // Unified materialization kernel lifecycle enums.
            "pangrova-materialization-core:GcLedgerEntry.GcState",
            "pangrova-materialization-core:PinState",
            "pangrova-materialization-core:ScheduleState",
            "pangrova-shuffle-service:ShuffleServiceModel.AllocationState",
            "pangrova-testing-benchmark:PerformanceRegressionDetector.Status"
    );

    private static final Pattern ENUM_STATE_PATTERN =
            Pattern.compile("\\benum\\s+(\\w*(?:State|Status)\\w*)\\s*\\{");

    private static final List<StateEnumEntry> ALL_STATE_ENUMS = scanStateEnums();

    // -----------------------------------------------------------------------
    // Property 32: SPI state enums have exactly one authoritative definition
    // -----------------------------------------------------------------------

    /**
     * Property 32: Each SPI-level state enum name must appear in exactly one
     * stable upstream module.
     */
    @Property(tries = 10)
    @Tag("Feature: fragmentation-audit, Property 32: State enum authoritative uniqueness")
    void spiStateEnumHasExactlyOneAuthoritativeDefinition(
            @ForAll("spiEnumNames") String enumName) {
        long count = ALL_STATE_ENUMS.stream()
                .filter(e -> e.enumName.equals(enumName))
                .filter(e -> STABLE_UPSTREAM_MODULES.contains(e.module))
                .count();
        assertThat(count)
                .as("SPI state enum '%s' must have exactly 1 definition in stable upstream modules %s, found %d",
                        enumName, STABLE_UPSTREAM_MODULES, count)
                .isEqualTo(1);
    }

    /**
     * Property 32: No unknown state enum definitions outside the known inventory.
     * Any new state enum must be registered in the known inventory to prevent
     * untracked fragmentation.
     */
    @Property(tries = 200)
    @Tag("Feature: fragmentation-audit, Property 32: State enum authoritative uniqueness")
    void allStateEnumsAreInKnownInventory(@ForAll("stateEnumEntries") StateEnumEntry entry) {
        String key = entry.module + ":" + entry.qualifiedEnumName;
        boolean isSpiAuthoritative = SPI_AUTHORITATIVE_ENUMS.contains(entry.enumName)
                && STABLE_UPSTREAM_MODULES.contains(entry.module);
        boolean isKnownInternal = KNOWN_INTERNAL_STATE_ENUMS.contains(key);

        assertThat(isSpiAuthoritative || isKnownInternal)
                .as("State enum '%s' in module '%s' (key='%s') is not in the known inventory. "
                                + "Register it in KNOWN_INTERNAL_STATE_ENUMS or SPI_AUTHORITATIVE_ENUMS "
                                + "to prevent untracked fragmentation.",
                        entry.qualifiedEnumName, entry.module, key)
                .isTrue();
    }

    /**
     * Property 32: No two stable upstream modules define the same state enum name.
     */
    @Property(tries = 10)
    @Tag("Feature: fragmentation-audit, Property 32: State enum authoritative uniqueness")
    void noSpiEnumDuplicatedAcrossStableModules(
            @ForAll("spiEnumNames") String enumName) {
        Map<String, List<StateEnumEntry>> byModule = new HashMap<>();
        for (StateEnumEntry entry : ALL_STATE_ENUMS) {
            if (entry.enumName.equals(enumName) && STABLE_UPSTREAM_MODULES.contains(entry.module)) {
                byModule.computeIfAbsent(entry.module, k -> new ArrayList<>()).add(entry);
            }
        }
        assertThat(byModule.size())
                .as("SPI enum '%s' found in multiple stable modules: %s", enumName, byModule.keySet())
                .isLessThanOrEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> spiEnumNames() {
        if (SPI_AUTHORITATIVE_ENUMS.isEmpty()) {
            return Arbitraries.just("QueryState");
        }
        return Arbitraries.of(new ArrayList<>(SPI_AUTHORITATIVE_ENUMS));
    }

    @Provide
    Arbitrary<StateEnumEntry> stateEnumEntries() {
        if (ALL_STATE_ENUMS.isEmpty()) {
            return Arbitraries.just(new StateEnumEntry("(none)", "(none)", "(none)", "(none)"));
        }
        return Arbitraries.of(ALL_STATE_ENUMS);
    }

    // -----------------------------------------------------------------------
    // Scanning
    // -----------------------------------------------------------------------

    private static List<StateEnumEntry> scanStateEnums() {
        List<StateEnumEntry> entries = new ArrayList<>();
        List<String> modules = findJavaModules();
        for (String modulePath : modules) {
            Path srcDir = PROJECT_ROOT.resolve(modulePath).resolve("src/main/java");
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            String moduleName = Path.of(modulePath).getFileName().toString();
            try (Stream<Path> files = Files.walk(srcDir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".java"))
                        .forEach(p -> scanFile(p, moduleName, entries));
            } catch (IOException ignored) {
                // skip inaccessible modules
            }
        }
        return List.copyOf(entries);
    }

    private static void scanFile(Path file, String module, List<StateEnumEntry> entries) {
        try {
            String content = Files.readString(file);
            Matcher matcher = ENUM_STATE_PATTERN.matcher(content);
            String fileName = file.getFileName().toString().replace(".java", "");
            while (matcher.find()) {
                String enumName = matcher.group(1);
                String qualifiedName = isNestedEnum(content, enumName, fileName)
                        ? fileName + "." + enumName
                        : enumName;
                entries.add(new StateEnumEntry(module, enumName, qualifiedName, file.toString()));
            }
        } catch (IOException ignored) {
            // skip unreadable files
        }
    }

    private static boolean isNestedEnum(String content, String enumName, String fileName) {
        return !enumName.equals(fileName);
    }

    private static List<String> findJavaModules() {
        List<String> modules = new ArrayList<>();
        // Modules live either at the repository root or one aggregator level below it.
        try (Stream<Path> dirs = Files.walk(PROJECT_ROOT, 2)) {
            dirs.filter(Files::isDirectory)
                    .filter(d -> !d.equals(PROJECT_ROOT))
                    .filter(d -> !d.toString().contains("target"))
                    .filter(d -> d.getFileName().toString().startsWith("pangrova-"))
                    .filter(d -> Files.isRegularFile(d.resolve("pom.xml")))
                    .filter(d -> Files.isDirectory(d.resolve("src/main/java")))
                    .forEach(d -> modules.add(PROJECT_ROOT.relativize(d).toString()));
        } catch (IOException ignored) {
            // skip
        }
        return List.copyOf(modules);
    }

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    record StateEnumEntry(String module, String enumName, String qualifiedEnumName, String filePath) {
        @Override
        public String toString() {
            return module + ":" + qualifiedEnumName;
        }
    }
}
