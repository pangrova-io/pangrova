package org.pangrova.indexing.artifact;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.indexing.control.ArtifactStageGraph;
import org.pangrova.indexing.control.ArtifactStageTemplate;
import org.pangrova.indexing.control.ArtifactStageTemplate.PublicationRead;
import org.pangrova.indexing.api.ArtifactBuildPageKind;
import org.pangrova.security.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactStageTemplateTest {
    private static final String SHA = "a".repeat(64);

    @Test
    void bindingDigestRequiresExactLowercaseHexAndSupportsActualEmptyBindings() {
        byte[] bindings = "stage binding contract".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(bindings);
        String expected = DigestUtils.sha256Hex(bindings);
        assertThat(new ArtifactStageTemplate(2, encoded, expected, List.of(), List.of()).bindingBytes())
                .containsExactly(bindings);
        assertThat(ArtifactStageTemplate.of(new byte[0], List.of(), List.of()).bindingBytes()).isEmpty();
        for (String invalid : new String[]{null, "", "0".repeat(64), expected.substring(1),
                expected.toUpperCase(java.util.Locale.ROOT), " " + expected}) {
            assertThatThrownBy(() -> new ArtifactStageTemplate(2, encoded, invalid, List.of(), List.of()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum differs");
        }
    }

    @Test
    void includesCanonicalPointersResultContractsAndExactModelIncarnationInGraphDigest() {
        var a = read("artifact-a");
        var b = read("artifact-b");
        var template = ArtifactStageTemplate.of(new byte[]{1}, List.of(new byte[]{2}), List.of(b, a));
        assertThat(template).isEqualTo(ArtifactStageTemplate.of(new byte[]{1}, List.of(new byte[]{2}), List.of(a, b)));
        var original = graph(template, new ModelSnapshot("model", "incarnation-1", 1, "v1", SHA));
        assertThat(ArtifactStageGraph.decode(original.canonicalBytes(), original.sha256())).isEqualTo(original);
        assertThat(original.sha256()).isNotEqualTo(graph(template,
                new ModelSnapshot("model", "incarnation-2", 1, "v1", SHA)).sha256());
        assertThat(original.sha256()).isNotEqualTo(graph(
                ArtifactStageTemplate.of(new byte[]{1}, List.of(new byte[]{3}), List.of(a, b)),
                original.modelSnapshots().get(0)).sha256());
    }

    @Test
    void rejectsDuplicatePublicationIdentityEvenWithDifferentManifestDigest() {
        var duplicate = new PublicationRead("artifact-a", "g1", "lake.raw.t", "41", "b".repeat(64));
        assertThatThrownBy(() -> ArtifactStageTemplate.of(new byte[]{1}, List.of(), List.of(read("artifact-a"), duplicate)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate");
    }

    @Test
    void rejectsOldFormatNoncanonicalBytesAndTamperedBindingChecksum() {
        assertThatThrownBy(() -> new ArtifactStageTemplate(1, "AQ==", DigestUtils.sha256Hex(new byte[]{1}), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rebuild");
        assertThatThrownBy(() -> new ArtifactStageTemplate(2, "AQ", DigestUtils.sha256Hex(new byte[]{1}), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical");
        assertThatThrownBy(() -> new ArtifactStageTemplate(2, "AQ==", SHA, List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> new ArtifactStageTemplate(2, "AQ==", DigestUtils.sha256Hex(new byte[]{1}),
                List.of(Base64.getEncoder().encodeToString(new byte[0])), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
    }

    @Test
    void onlyOneCurrentGraphReaderRejectsMissingFormatRatherThanSynthesizingATemplate() {
        byte[] old = "{\"purpose\":\"RELATIONAL_EXPORT\",\"stages\":[],\"exchanges\":[]}"
                .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ArtifactStageGraph.decode(old, DigestUtils.sha256Hex(old)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid")
                .hasRootCauseMessage("Durable graph requires format 2; explicitly rebuild old jobs");
    }

    private static PublicationRead read(String artifact) {
        return new PublicationRead(artifact, "g1", "lake.raw.t", "41", SHA);
    }

    private static ArtifactStageGraph graph(ArtifactStageTemplate template, ModelSnapshot model) {
        var stage = ArtifactStageGraph.StagePlan.of(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, template, List.of("lake.raw.t"), List.of("value"), true);
        return new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT, List.of(stage), List.of(), List.of(model));
    }
}
