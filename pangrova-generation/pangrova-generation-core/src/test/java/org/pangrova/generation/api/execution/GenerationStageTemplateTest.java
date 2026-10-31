package org.pangrova.generation.api.execution;

import org.pangrova.generation.protocol.GenerationStageGraphCodec;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.execution.template.GenerationStageTemplate;
import org.pangrova.generation.api.execution.template.GenerationStageTemplate.PublicationRead;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationStageTemplateTest {
    private static final String SHA = "a".repeat(64);

    @Test
    void bindingDigestRequiresExactLowercaseHexAndSupportsActualEmptyBindings() {
        byte[] bindings = "stage binding contract".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(bindings);
        String expected = DigestUtils.sha256Hex(bindings);
        assertThat(new GenerationStageTemplate(2, encoded, expected, List.of(), List.of()).bindingBytes())
                .containsExactly(bindings);
        assertThat(GenerationStageTemplate.of(new byte[0], List.of(), List.of()).bindingBytes()).isEmpty();
        for (String invalid : new String[]{null, "", "0".repeat(64), expected.substring(1),
                expected.toUpperCase(java.util.Locale.ROOT), " " + expected}) {
            assertThatThrownBy(() -> new GenerationStageTemplate(2, encoded, invalid, List.of(), List.of()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum differs");
        }
    }

    @Test
    void includesCanonicalPointersResultContractsAndExactModelIncarnationInGraphDigest() {
        var a = read("artifact-a");
        var b = read("artifact-b");
        var template = GenerationStageTemplate.of(new byte[]{1}, List.of(new byte[]{2}), List.of(b, a));
        assertThat(template).isEqualTo(GenerationStageTemplate.of(new byte[]{1}, List.of(new byte[]{2}), List.of(a, b)));
        var original = graph(template, new ModelSnapshot("model", "incarnation-1", 1, "v1", SHA));
        assertThat(GenerationStageGraphCodec.decode(GenerationStageGraphCodec.encode(original), GenerationStageGraphCodec.sha256(original))).isEqualTo(original);
        assertThat(GenerationStageGraphCodec.sha256(original)).isNotEqualTo(GenerationStageGraphCodec.sha256(graph(template,
                new ModelSnapshot("model", "incarnation-2", 1, "v1", SHA))));
        assertThat(GenerationStageGraphCodec.sha256(original)).isNotEqualTo(GenerationStageGraphCodec.sha256(graph(
                GenerationStageTemplate.of(new byte[]{1}, List.of(new byte[]{3}), List.of(a, b)),
                original.modelSnapshots().get(0))));
    }

    @Test
    void rejectsDuplicatePublicationIdentityEvenWithDifferentManifestDigest() {
        var duplicate = new PublicationRead("artifact-a", "g1", "lake.raw.t", "41", "b".repeat(64));
        assertThatThrownBy(() -> GenerationStageTemplate.of(new byte[]{1}, List.of(), List.of(read("artifact-a"), duplicate)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate");
    }

    @Test
    void rejectsOldFormatNoncanonicalBytesAndTamperedBindingChecksum() {
        assertThatThrownBy(() -> new GenerationStageTemplate(1, "AQ==", DigestUtils.sha256Hex(new byte[]{1}), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rebuild");
        assertThatThrownBy(() -> new GenerationStageTemplate(2, "AQ", DigestUtils.sha256Hex(new byte[]{1}), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical");
        assertThatThrownBy(() -> new GenerationStageTemplate(2, "AQ==", SHA, List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> new GenerationStageTemplate(2, "AQ==", DigestUtils.sha256Hex(new byte[]{1}),
                List.of(Base64.getEncoder().encodeToString(new byte[0])), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
    }

    @Test
    void onlyOneCurrentGraphReaderRejectsMissingFormatRatherThanSynthesizingATemplate() {
        byte[] old = "{\"purpose\":\"RELATIONAL_EXPORT\",\"stages\":[],\"exchanges\":[]}"
                .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> GenerationStageGraphCodec.decode(old, DigestUtils.sha256Hex(old)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid")
                .hasRootCauseMessage("Durable graph requires format 2; explicitly rebuild old jobs");
    }

    private static PublicationRead read(String artifact) {
        return new PublicationRead(artifact, "g1", "lake.raw.t", "41", SHA);
    }

    private static GenerationStageGraph graph(GenerationStageTemplate template, ModelSnapshot model) {
        var stage = GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, template, List.of("lake.raw.t"), List.of("value"), true);
        return new GenerationStageGraph(2, GenerationStageGraph.Purpose.RELATIONAL_EXPORT, List.of(stage), List.of(), List.of(model));
    }
}
