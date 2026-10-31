package org.pangrova.databuild.assembly;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.api.execution.DataBuildStageGraph.StagePlan;
import org.pangrova.databuild.api.execution.DataBuildStageGraph.Purpose;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Assembles immutable executable stages from the submitted artifact page cohort. */
@Internal
public final class DataBuildStageGraphFactory {
    private DataBuildStageGraphFactory() {}

    /** Maps ordinary artifact page producers into the same immutable stage contract. */
    public static DataBuildStageGraph forArtifactPages(byte[] plan, byte[] bindings, List<DataBuildPage> pages,
            List<ModelSnapshot> modelSnapshots) {
        Map<Integer, List<DataBuildPage>> grouped = pages.stream().collect(
                java.util.stream.Collectors.groupingBy(DataBuildPage::stageId));
        List<StagePlan> stages = new ArrayList<>();
        for (var entry : grouped.entrySet()) {
            var kind = entry.getValue().get(0).kind();
            if (entry.getValue().stream().anyMatch(page -> page.kind() != kind)) {
                throw new IllegalArgumentException("One durable stage cannot mix page kinds");
            }
            int parallelism = entry.getValue().stream().mapToInt(DataBuildPage::partitionId)
                    .max().orElseThrow() + 1;
            stages.add(StagePlan.of(entry.getKey(), parallelism, kind, plan,
                    DataBuildStageTemplate.of(bindings, List.of(), List.of()), List.of(), List.of(),
                    kind != DataBuildPageKind.DEPENDENCY_REDUCE));
        }
        if (stages.isEmpty()) {
            stages.add(StagePlan.of(0, 0, DataBuildPageKind.SOURCE_MAP, plan,
                    DataBuildStageTemplate.of(bindings, List.of(), List.of()),
                    List.of(), List.of(), true));
        }
        return new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, Purpose.ARTIFACT_GENERATION,
                stages, List.of(), modelSnapshots);
    }

}
