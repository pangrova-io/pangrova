package org.pangrova.generation.codec.execution;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;

import org.junit.jupiter.api.Test;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.generation.api.enums.GenerationPageKind;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationStageGraphTest {

    @Test
    void graphRejectsMalformedDigestsAndCorrectlyHashedNoncanonicalEncoding() {
        var terminal = stage(0, 1, GenerationPageKind.RELATIONAL_PARTITION, true);
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                GenerationGraphPurpose.RELATIONAL_EXPORT, List.of(terminal), List.of(), List.of());
        for (String invalid : new String[]{null, "", "0".repeat(64), GenerationStageGraphCodec.sha256(graph).substring(1),
                GenerationStageGraphCodec.sha256(graph).toUpperCase(java.util.Locale.ROOT), GenerationStageGraphCodec.sha256(graph) + "\n"}) {
            assertThatThrownBy(() -> GenerationStageGraphCodec.decode(GenerationStageGraphCodec.encode(graph), invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        }
        byte[] canonical = GenerationStageGraphCodec.encode(graph);
        byte[] noncanonical = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        noncanonical[canonical.length] = '\n';
        assertThatThrownBy(() -> GenerationStageGraphCodec.decode(noncanonical, Sha256Digests.sha256Hex(noncanonical)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not canonical");
        assertThatThrownBy(() -> GenerationStageGraphCodec.decode(null, GenerationStageGraphCodec.sha256(graph)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bytes or checksum");
    }

    @Test
    void stagePlanRejectsWrongDigestWithoutAcceptingNoncanonicalBase64() {
        var original = stage(0, 1, GenerationPageKind.RELATIONAL_PARTITION, true);
        assertThatThrownBy(() -> new GenerationStageGraph.StagePlan(original.stageId(), original.parallelism(),
                original.pageKind(), original.planBase64(), "0".repeat(64), original.template(),
                original.sourceKeys(), original.outputNames(), original.publicationOutput()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("different checksum");
        String unpadded = original.planBase64().replace("=", "");
        assertThatThrownBy(() -> new GenerationStageGraph.StagePlan(original.stageId(), original.parallelism(),
                original.pageKind(), unpadded, original.planSha256(), original.template(),
                original.sourceKeys(), original.outputNames(), original.publicationOutput()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical");
    }

    @Test void rejectsRemovedOrUnknownPersistedBatchFormats() {
        for (String format : List.of("BATCH_SERDE_NATIVE_ROW", "BATCH_SERDE_UNSPECIFIED", "BATCH_SERDE_UNKNOWN")) {
            assertThatThrownBy(() -> new GenerationStageGraph.Exchange("exchange", 1, 0,
                    GenerationExchangeDistribution.SINGLETON, List.of(), 1, format, true))
                    .as("persisted format %s", format).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test void roundTripsOneCanonicalGraphAndIncludesEveryStagePayloadAndCohort() {
        var source = stage(5, 2, GenerationPageKind.RELATIONAL_COMPUTE, false);
        var terminal = stage(0, 3, GenerationPageKind.RELATIONAL_PARTITION, true);
        var exchange = new GenerationStageGraph.Exchange("exchange", 5, 0,
                GenerationExchangeDistribution.HASH, List.of(0), 3,
                "BATCH_SERDE_ARROW_BATCH", true);
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.RELATIONAL_EXPORT,
                List.of(source, terminal), List.of(exchange), List.of());
        assertThat(GenerationStageGraphCodec.decode(GenerationStageGraphCodec.encode(graph), GenerationStageGraphCodec.sha256(graph))).isEqualTo(graph);
        assertThat(graph.stages()).extracting(GenerationStageGraph.StagePlan::stageId).containsExactly(0, 5);
        assertThat(graph.requireStage(5).sourceKeys()).containsExactly("lake.raw.events", "lake.raw.accounts");
        byte[] corrupt = GenerationStageGraphCodec.encode(graph); corrupt[0] ^= 1;
        assertThatThrownBy(() -> GenerationStageGraphCodec.decode(corrupt, GenerationStageGraphCodec.sha256(graph)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> graph.requireStage(99)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectsPartialBarriersPurposeMismatchAndDetachedCompute() {
        assertThatThrownBy(() -> new GenerationStageGraph.Exchange("exchange", 1, 0,
                GenerationExchangeDistribution.SINGLETON, List.of(), 1,
                "BATCH_SERDE_ARROW_BATCH", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.RELATIONAL_EXPORT,
                List.of(stage(1, 1, GenerationPageKind.RELATIONAL_COMPUTE, false),
                        stage(0, 1, GenerationPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("detached");
        assertThatThrownBy(() -> new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.ARTIFACT_GENERATION,
                List.of(stage(0, 1, GenerationPageKind.RELATIONAL_PARTITION, true)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("purpose");
    }

    @Test
    void freezesExactSplittableExchangeIdsInsideTheGraphDigest() {
        var template = GenerationStageTemplate.of(new byte[0], List.of(), List.of());
        var left = stage(1, 2, GenerationPageKind.RELATIONAL_COMPUTE, false);
        var right = stage(2, 2, GenerationPageKind.RELATIONAL_COMPUTE, false);
        var terminal = stage(4, 1, GenerationPageKind.RELATIONAL_PARTITION, true);
        var edges = List.of(
                new GenerationStageGraph.Exchange("left", 1, 3, GenerationExchangeDistribution.HASH,
                        List.of(0), 2, "BATCH_SERDE_ARROW_BATCH", true),
                new GenerationStageGraph.Exchange("right", 2, 3, GenerationExchangeDistribution.HASH,
                        List.of(0), 2, "BATCH_SERDE_ARROW_BATCH", true),
                new GenerationStageGraph.Exchange("result", 3, 4, GenerationExchangeDistribution.SINGLETON,
                        List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true));
        java.util.function.Function<List<String>, GenerationStageGraph> graph = allowed -> new GenerationStageGraph(
                GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.RELATIONAL_EXPORT,
                List.of(left, right, GenerationStageGraph.StagePlan.of(3, 2, GenerationPageKind.RELATIONAL_COMPUTE,
                        new byte[]{7}, template, List.of(), List.of("id"), false, true, allowed), terminal),
                edges, List.of());

        var frozen = graph.apply(List.of("right", "left"));
        var digest = GenerationStageGraphCodec.sha256(frozen);
        assertThat(GenerationStageGraphCodec.decode(GenerationStageGraphCodec.encode(frozen), digest)
                .requireStage(3).splittableHashInputs()).containsExactly("left", "right");
        assertThat(GenerationStageGraphCodec.sha256(graph.apply(List.of("left")))).isNotEqualTo(digest);
        assertThatThrownBy(() -> graph.apply(List.of("result"))).hasMessageContaining("incoming HASH");
        assertThatThrownBy(() -> graph.apply(List.of("left", "left"))).hasMessageContaining("skew eligibility");
    }

    private static GenerationStageGraph.StagePlan stage(int id, int parallelism,
            GenerationPageKind kind, boolean terminal) {
        return GenerationStageGraph.StagePlan.of(id, parallelism, kind, new byte[] {1, 2},
                GenerationStageTemplate.of(terminal ? new byte[] {3} : new byte[0], List.of(), List.of()),
                terminal ? List.of() : List.of("lake.raw.events", "lake.raw.accounts"), List.of("id"), terminal);
    }
}
