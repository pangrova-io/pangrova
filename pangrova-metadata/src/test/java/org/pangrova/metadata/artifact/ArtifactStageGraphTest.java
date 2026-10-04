package org.pangrova.metadata.artifact;

import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactStageGraphTest {

    @Test void rejectsRemovedOrUnknownPersistedBatchFormats() {
        for (String format : List.of("BATCH_SERDE_NATIVE_ROW", "BATCH_SERDE_UNSPECIFIED", "BATCH_SERDE_UNKNOWN")) {
            assertThatThrownBy(() -> new ArtifactStageGraph.Exchange("exchange", 1, 0,
                    ArtifactStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1, format, true))
                    .as("persisted format %s", format).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test void roundTripsOneCanonicalGraphAndIncludesEveryStagePayloadAndCohort() {
        var source = stage(5, 2, ArtifactBuildPageKind.RELATIONAL_COMPUTE, false);
        var terminal = stage(0, 3, ArtifactBuildPageKind.RELATIONAL_PARTITION, true);
        var exchange = new ArtifactStageGraph.Exchange("exchange", 5, 0,
                ArtifactStageGraph.Exchange.Distribution.HASH, List.of(0), 3,
                "BATCH_SERDE_ARROW_BATCH", true);
        var graph = new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(source, terminal), List.of(exchange), List.of());
        assertThat(ArtifactStageGraph.decode(graph.canonicalBytes(), graph.sha256())).isEqualTo(graph);
        assertThat(graph.stages()).extracting(ArtifactStageGraph.StagePlan::stageId).containsExactly(0, 5);
        assertThat(graph.requireStage(5).sourceKeys()).containsExactly("lake.raw.events", "lake.raw.accounts");
        byte[] corrupt = graph.canonicalBytes(); corrupt[0] ^= 1;
        assertThatThrownBy(() -> ArtifactStageGraph.decode(corrupt, graph.sha256()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> graph.requireStage(99)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectsPartialBarriersPurposeMismatchAndDetachedCompute() {
        assertThatThrownBy(() -> new ArtifactStageGraph.Exchange("exchange", 1, 0,
                ArtifactStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1,
                "BATCH_SERDE_ARROW_BATCH", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(stage(1, 1, ArtifactBuildPageKind.RELATIONAL_COMPUTE, false),
                        stage(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("detached");
        assertThatThrownBy(() -> new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION, ArtifactStageGraph.Purpose.ARTIFACT_GENERATION,
                List.of(stage(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("purpose");
    }

    private static ArtifactStageGraph.StagePlan stage(int id, int parallelism,
            ArtifactBuildPageKind kind, boolean terminal) {
        return ArtifactStageGraph.StagePlan.of(id, parallelism, kind, new byte[] {1, 2},
                ArtifactStageTemplate.of(terminal ? new byte[] {3} : new byte[0], List.of(), List.of()),
                terminal ? List.of() : List.of("lake.raw.events", "lake.raw.accounts"), List.of("id"), terminal);
    }
}
