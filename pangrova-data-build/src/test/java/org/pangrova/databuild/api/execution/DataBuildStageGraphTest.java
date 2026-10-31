package org.pangrova.databuild.api.execution;

import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;

import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.databuild.api.DataBuildPageKind;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBuildStageGraphTest {

    @Test
    void graphRejectsMalformedDigestsAndCorrectlyHashedNoncanonicalEncoding() {
        var terminal = stage(0, 1, DataBuildPageKind.RELATIONAL_PARTITION, true);
        var graph = new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION,
                DataBuildStageGraph.Purpose.RELATIONAL_EXPORT, List.of(terminal), List.of(), List.of());
        for (String invalid : new String[]{null, "", "0".repeat(64), DataBuildStageGraphCodec.sha256(graph).substring(1),
                DataBuildStageGraphCodec.sha256(graph).toUpperCase(java.util.Locale.ROOT), DataBuildStageGraphCodec.sha256(graph) + "\n"}) {
            assertThatThrownBy(() -> DataBuildStageGraphCodec.decode(DataBuildStageGraphCodec.encode(graph), invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        }
        byte[] canonical = DataBuildStageGraphCodec.encode(graph);
        byte[] noncanonical = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        noncanonical[canonical.length] = '\n';
        assertThatThrownBy(() -> DataBuildStageGraphCodec.decode(noncanonical, DigestUtils.sha256Hex(noncanonical)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not canonical");
        assertThatThrownBy(() -> DataBuildStageGraphCodec.decode(null, DataBuildStageGraphCodec.sha256(graph)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bytes or checksum");
    }

    @Test
    void stagePlanRejectsWrongDigestWithoutAcceptingNoncanonicalBase64() {
        var original = stage(0, 1, DataBuildPageKind.RELATIONAL_PARTITION, true);
        assertThatThrownBy(() -> new DataBuildStageGraph.StagePlan(original.stageId(), original.parallelism(),
                original.pageKind(), original.planBase64(), "0".repeat(64), original.template(),
                original.sourceKeys(), original.outputNames(), original.publicationOutput()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different checksum");
        String unpadded = original.planBase64().replace("=", "");
        assertThatThrownBy(() -> new DataBuildStageGraph.StagePlan(original.stageId(), original.parallelism(),
                original.pageKind(), unpadded, original.planSha256(), original.template(),
                original.sourceKeys(), original.outputNames(), original.publicationOutput()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical");
    }

    @Test void rejectsRemovedOrUnknownPersistedBatchFormats() {
        for (String format : List.of("BATCH_SERDE_NATIVE_ROW", "BATCH_SERDE_UNSPECIFIED", "BATCH_SERDE_UNKNOWN")) {
            assertThatThrownBy(() -> new DataBuildStageGraph.Exchange("exchange", 1, 0,
                    DataBuildStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1, format, true))
                    .as("persisted format %s", format).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test void roundTripsOneCanonicalGraphAndIncludesEveryStagePayloadAndCohort() {
        var source = stage(5, 2, DataBuildPageKind.RELATIONAL_COMPUTE, false);
        var terminal = stage(0, 3, DataBuildPageKind.RELATIONAL_PARTITION, true);
        var exchange = new DataBuildStageGraph.Exchange("exchange", 5, 0,
                DataBuildStageGraph.Exchange.Distribution.HASH, List.of(0), 3,
                "BATCH_SERDE_ARROW_BATCH", true);
        var graph = new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(source, terminal), List.of(exchange), List.of());
        assertThat(DataBuildStageGraphCodec.decode(DataBuildStageGraphCodec.encode(graph), DataBuildStageGraphCodec.sha256(graph))).isEqualTo(graph);
        assertThat(graph.stages()).extracting(DataBuildStageGraph.StagePlan::stageId).containsExactly(0, 5);
        assertThat(graph.requireStage(5).sourceKeys()).containsExactly("lake.raw.events", "lake.raw.accounts");
        byte[] corrupt = DataBuildStageGraphCodec.encode(graph); corrupt[0] ^= 1;
        assertThatThrownBy(() -> DataBuildStageGraphCodec.decode(corrupt, DataBuildStageGraphCodec.sha256(graph)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> graph.requireStage(99)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectsPartialBarriersPurposeMismatchAndDetachedCompute() {
        assertThatThrownBy(() -> new DataBuildStageGraph.Exchange("exchange", 1, 0,
                DataBuildStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1,
                "BATCH_SERDE_ARROW_BATCH", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(stage(1, 1, DataBuildPageKind.RELATIONAL_COMPUTE, false),
                        stage(0, 1, DataBuildPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("detached");
        assertThatThrownBy(() -> new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, DataBuildStageGraph.Purpose.ARTIFACT_GENERATION,
                List.of(stage(0, 1, DataBuildPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("purpose");
    }

    private static DataBuildStageGraph.StagePlan stage(int id, int parallelism,
            DataBuildPageKind kind, boolean terminal) {
        return DataBuildStageGraph.StagePlan.of(id, parallelism, kind, new byte[] {1, 2},
                DataBuildStageTemplate.of(terminal ? new byte[] {3} : new byte[0], List.of(), List.of()),
                terminal ? List.of() : List.of("lake.raw.events", "lake.raw.accounts"), List.of("id"), terminal);
    }
}
