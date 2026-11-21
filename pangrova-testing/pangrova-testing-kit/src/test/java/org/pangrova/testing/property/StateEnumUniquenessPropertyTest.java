package org.pangrova.testing.property;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.pangrova.testing.testkit.architecture.JavaSourceReferences;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks every declared state enum against its production module and semantic owner. */
class StateEnumUniquenessPropertyTest {
    private static final Set<String> CONTRACT_ENUMS = Set.of(
            "TableControlState", "MaintenanceRunState", "IndexState",
            "QueryState",
            "ReportedTaskState",
            "StatementCategory", // Shared Query API observation; not a second query lifecycle.
            "StageState",
            "ReadSnapshotState",
            "TransportStatus",
            "GenerationJobState",
            "ObjectState", "GenerationState", "AttemptState", "CdcStreamState",
            "WorkerHeartbeatStatus"
    );

    private static final Set<String> DOMAIN_ENUMS = Set.of(
            "pangrova-streaming-pipeline:CdcBatchPublicationState",
            "pangrova-coordination-control:ScheduledTaskState",
            "pangrova-scheduler-api:HealthState",
            "pangrova-worker:TaskState",
            // One-shot local assembly admission/cleanup phases, not distributed Worker health or task state.
            "pangrova-worker:WorkerBootstrapState",
            "pangrova-shuffle-pangrova:ProducerStreamState",
            "pangrova-shuffle-pangrova:TaskEgressState",
            "pangrova-operations:DdlJobState",
            "pangrova-metadata-api:CatalogState",
            "pangrova-streaming-pipeline:WatermarkState",
            "pangrova-security-api:TenantStatus",
            "pangrova-coordination-control:SplitRetryState",
            "pangrova-connector-api:ConnectorHealthStatus",
            "pangrova-observability-api:StatusCode",  // trace outcome, not query/task lifecycle
            "pangrova-planner:ValidationStatus",
            "pangrova-coordination-control:SchedulerCancellationStatus",
            "pangrova-security-api:AuthenticatorHealthStatus",
            "pangrova-worker:TaskCancellationStatus",
            "pangrova-shuffle-exchange:SpoolSinkState",
            "pangrova-streaming-api:StreamLifecycleState",
            "pangrova-semantic:SemanticViewValidationState",
            "pangrova-ai-control:EvaluationRunState",
            "pangrova-connector-api:ClusteringState",
            "pangrova-coordination-control:ReclusterJobState",  // data recluster lifecycle
            "pangrova-streaming-api:CdcCommitState",  // CDC two-phase commit coordinator (Req 10.6)
            "pangrova-operations:JfrState",  // JFR profiling lifecycle
            "pangrova-indexing-lifecycle:IndexFileReadStatus",  // index file read result status
            "pangrova-persistence-query-api:WriteCommitState",  // distributed write commit lifecycle
            // Unified materialization kernel lifecycle enums.
            "pangrova-materialization-api:GcState",
            "pangrova-materialization-api:PinState",
            "pangrova-materialization-api:ScheduleState",
            "pangrova-shuffle-service:ShuffleAllocationState",
            "pangrova-testing-benchmark:PerformanceRegressionStatus"
    );

    private static final java.util.regex.Pattern STATE_TERM =
            java.util.regex.Pattern.compile("(?:State|Status)(?:[A-Z]|$)");

    private static final Set<String> FLAT_CONTRACT_MODULES = Set.of(
            "pangrova-protocol", "pangrova-parser", "pangrova-batch", "pangrova-file", "pangrova-planner",
            "pangrova-query", "pangrova-table", "pangrova-semantic");

    @Test
    void everyStateEnumHasAnOwner() throws Exception {
        Path root = TestProjectPaths.findProjectRoot();
        var roots = PackageDependencyGraph.sourceRoots(root);
        var facts = JavaSourceReferences.scan(roots);
        var internal = new TreeSet<String>();
        var authoritative = new java.util.TreeMap<String, List<String>>();
        for (Path source : roots) {
            String module = source.getParent().getParent().getParent().getFileName().toString();
            assertThat(facts.get(source).misplacedDeclarations())
                    .as("Place named production enums, exceptions and pure constants in their owning package in %s", module)
                    .isEmpty();
            for (String qualified : facts.get(source).enumDeclarations()) {
                String simple = qualified.substring(qualified.lastIndexOf('.') + 1);
                if (CONTRACT_ENUMS.contains(simple)) {
                    assertThat(module.endsWith("-api") || FLAT_CONTRACT_MODULES.contains(module))
                            .as("%s is a shared contract declared in %s", qualified, module).isTrue();
                    authoritative.computeIfAbsent(simple, ignored -> new ArrayList<>()).add(module + ":" + qualified);
                } else if (STATE_TERM.matcher(simple).find()) {
                    String type = qualified.substring(packageName(qualified, facts.get(source).packages()).length() + 1);
                    internal.add(module + ":" + type);
                }
            }
        }
        assertThat(authoritative.keySet()).containsExactlyInAnyOrderElementsOf(CONTRACT_ENUMS);
        authoritative.forEach((name, owners) -> assertThat(owners).as("Definitions of %s", name).hasSize(1));
        assertThat(internal).as("Add new state owners and remove retired ones from the inventory")
                .containsExactlyInAnyOrderElementsOf(DOMAIN_ENUMS);
    }

    private static String packageName(String type, Set<String> packages) {
        String candidate = type;
        while (!packages.contains(candidate)) {
            candidate = candidate.substring(0, candidate.lastIndexOf('.'));
        }
        return candidate;
    }
}
