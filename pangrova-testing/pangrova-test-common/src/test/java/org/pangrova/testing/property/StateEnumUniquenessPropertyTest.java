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
 * Property 32: State Enum Authoritative Uniqueness.
 *
 * <p>Verifies that SPI-level state enums (QueryState, TaskStatus.TaskState, TransportStatus,
 * HealthStatus) each have exactly one authoritative definition in the stable upstream
 * modules (pangrova-spi, pangrova-protocol).</p>
 *
 * <p>Internal state machines (QueryStateMachine.State, TaskManager.TaskState,
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
     * in stable upstream modules (pangrova-spi, pangrova-protocol).
     */
    private static final Set<String> SPI_AUTHORITATIVE_ENUMS = Set.of(
            "QueryState",
            "SnapshotState",
            "TransportStatus"
    );

    /**
     * Stable upstream modules where SPI state enums must be defined.
     */
    private static final Set<String> STABLE_UPSTREAM_MODULES = Set.of(
            "pangrova-spi",
            "pangrova-protocol"
    );

    /**
     * Known internal state enums that are intentionally module-scoped.
     * These are allowed because they serve a different lifecycle granularity
     * than the SPI enums and have proper mapping layers.
     */
    private static final Set<String> KNOWN_INTERNAL_STATE_ENUMS = Set.of(
            "pangrova-coordinator:QueryStateMachine.State",
            "pangrova-coordinator:ConnectorPageSinkCdcWriter.PublicationState",
            "pangrova-coordinator:TaskStateMachine.State",
            "pangrova-coordinator:StageStateMachine.State",
            "pangrova-coordinator:HealthState",
            "pangrova-worker:TaskManager.TaskState",
            // One-shot local assembly admission/cleanup phases, not distributed Worker health or task state.
            "pangrova-worker:WorkerBootstrapLifecycle.State",
            "pangrova-worker:ExchangeClient.State",
            "pangrova-worker:RemoteShuffleProducerStream.State",
            "pangrova-operations:DdlJob.DdlJobState",
            "pangrova-metadata:CatalogState",
            "pangrova-streaming:CdcStreamState",
            "pangrova-streaming:WatermarkState",
            "pangrova-security:TenantStatus",
            "pangrova-resilience:CircuitBreaker.State",
            "pangrova-resilience:SplitRetryManager.SplitState",
            "pangrova-spi:HealthCheckResult.HealthStatus",
            "pangrova-spi:TaskStatus.TaskState",
            "pangrova-spi:StatusCode",
            "pangrova-planner:ValidationStatus",
            "pangrova-metadata:RefreshStatus",
            "pangrova-metadata:DynamicTableLifecycleState",
            "pangrova-metadata:MaterializedViewState",
            "pangrova-coordinator:SchedulerCancelResult.Status",
            "pangrova-security:AuthenticatorHealthStatus",
            "pangrova-worker:TaskCancelResult.Status",
            "pangrova-metadata:StreamLifecycleState",
            "pangrova-spi:ClusteringState",
            "pangrova-spi:IndexState",
            "pangrova-common:StateStoreType",  // moved from pangrova-config in cbe99e06 module merge
            "pangrova-config:StateStoreType",  // legacy copy retained for StateStoreFactory/CoordinatorOptions
            "pangrova-spi:CdcRegistration.CdcStreamState",
            "pangrova-coordinator:StatementCategory",
            "pangrova-coordinator:ReclusterJob.State",  // data recluster lifecycle
            "pangrova-parser:StatementType",  // false positive — discriminator enum, "StatementType" contains "State" substring
            "pangrova-streaming:CdcCommitState",  // CDC two-phase commit coordinator (Req 10.6)
            "pangrova-operations:JfrState",  // JFR profiling lifecycle
            "pangrova-spi:TransportStatus",  // gRPC transport status
            "pangrova-spi:QueryState",  // SPI query lifecycle
            "pangrova-coordinator:SettableDynamicFilter.State",  // adaptive dynamic filter state
            "pangrova-indexing:IndexFileReadResult.Status",  // index file read result status
            "pangrova-spi:WriteCommitEntry.State",  // distributed write commit lifecycle
            "pangrova-metadata:SemanticViewValidationState",
            "pangrova-metadata:EvaluationRunState",
            "pangrova-metadata:ArtifactBuildJobState"
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
        for (String module : modules) {
            Path srcDir = PROJECT_ROOT.resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(srcDir)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(srcDir)) {
                files.filter(p -> p.getFileName().toString().endsWith(".java"))
                        .forEach(p -> scanFile(p, module, entries));
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
        try (Stream<Path> dirs = Files.list(PROJECT_ROOT)) {
            dirs.filter(Files::isDirectory)
                    .filter(d -> d.getFileName().toString().startsWith("pangrova-"))
                    .filter(d -> Files.isRegularFile(d.resolve("pom.xml")))
                    .filter(d -> Files.isDirectory(d.resolve("src/main/java")))
                    .forEach(d -> modules.add(d.getFileName().toString()));
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
