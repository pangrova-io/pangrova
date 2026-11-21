package org.pangrova.generation.submission;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Assembles immutable executable stages from the submitted artifact page cohort. */
@Internal
public final class GenerationStageGraphFactory {
    private GenerationStageGraphFactory() {}

    /** Maps ordinary artifact page producers into the same immutable stage contract. */
    public static GenerationStageGraph forArtifactPages(byte[] plan, byte[] bindings, List<GenerationPage> pages,
            List<ModelSnapshot> modelSnapshots) {
        Map<Integer, List<GenerationPage>> grouped = pages.stream().collect(
                java.util.stream.Collectors.groupingBy(GenerationPage::stageId));
        List<StagePlan> stages = new ArrayList<>();
        for (var entry : grouped.entrySet()) {
            var kind = entry.getValue().get(0).kind();
            if (entry.getValue().stream().anyMatch(page -> page.kind() != kind)) {
                throw new IllegalArgumentException("One durable stage cannot mix page kinds");
            }
            int parallelism = entry.getValue().stream().mapToInt(GenerationPage::partitionId)
                    .max().orElseThrow() + 1;
            stages.add(StagePlan.of(entry.getKey(), parallelism, kind, plan,
                    GenerationStageTemplate.of(bindings, List.of(), List.of()), List.of(), List.of(),
                    kind != GenerationPageKind.DEPENDENCY_REDUCE));
        }
        if (stages.isEmpty()) {
            stages.add(StagePlan.of(0, 0, GenerationPageKind.SOURCE_MAP, plan,
                    GenerationStageTemplate.of(bindings, List.of(), List.of()),
                    List.of(), List.of(), true));
        }
        return new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.ARTIFACT_GENERATION,
                stages, List.of(), modelSnapshots);
    }

}
