package org.pangrova.generation.protocol;

import org.pangrova.generation.assembly.GenerationStageGraphFactory;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.execution.GenerationExchangeFinalization;
import org.pangrova.generation.api.execution.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.execution.GenerationStageGraph;
import org.pangrova.generation.api.execution.template.GenerationStageTemplate;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationEncodingCompatibilityTest {
    private static GenerationStageGraph graph() {
        var read = new GenerationStageTemplate.PublicationRead("artifact-输入", "generation-1", "lake.source", "41", "d".repeat(64));
        var template = GenerationStageTemplate.of(new byte[]{3, 4}, List.of(new byte[]{5, 6}), List.of(read));
        var source = GenerationStageGraph.StagePlan.of(5, 2, GenerationPageKind.RELATIONAL_COMPUTE,
                new byte[]{1, 2}, template, List.of("lake.源"), List.of("id", "value"), false);
        var terminal = GenerationStageGraph.StagePlan.of(0, 3, GenerationPageKind.RELATIONAL_PARTITION,
                new byte[]{7, 8}, template, List.of(), List.of("id", "value"), true);
        var edge = new GenerationStageGraph.Exchange("edge-1", 5, 0, GenerationStageGraph.Exchange.Distribution.HASH,
                List.of(0), 3, "BATCH_SERDE_ARROW_BATCH", true);
        return new GenerationStageGraph(2, GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(source, terminal), List.of(edge),
                List.of(new ModelSnapshot("model-文本", "incarnation", 7, "deployment-v1", "a".repeat(64))));
    }
    private static GenerationExchangeRegistration registration() {
        var a = new GenerationExchangeRegistration.Address("shuffle-a", 9080, "identity-a");
        var b = new GenerationExchangeRegistration.Address("shuffle-b", 9081, "identity-b");
        return new GenerationExchangeRegistration("edge-1", "artifact:execution", "shuffle-实际", 5, 7,
                "a".repeat(64), "b".repeat(64), "snapshot-41", 2, "c".repeat(64), 2,
                List.of(new GenerationExchangeRegistration.Endpoint(b, 1, 2),
                        new GenerationExchangeRegistration.Endpoint(a, 0, 1)), a,
                new GenerationExecutionOwner("process", "incarnation", 3));
    }
    private static GenerationExchangeFinalization finalization() {
        var r = registration();
        byte[] response = {8, 1};
        return new GenerationExchangeFinalization(r.exchangeId(), GenerationExchangeEvidenceCodec.sha256(r), r.shuffleId(), 7,
                r.registeredOwner(), 2, r.producerSetSha256(),
                List.of(new GenerationExchangeFinalization.Partition(1, 10, 2, 0, "d".repeat(64)),
                        new GenerationExchangeFinalization.Partition(0, 0, 0, -1, "e".repeat(64))),
                10, 2, Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }
    @Test
    void graphKeepsPersistedBytesAndDigest() throws Exception {
        byte[] expected = golden("stage-graph-v2.json");
        assertThat(GenerationStageGraphCodec.encode(graph())).isEqualTo(expected);
        assertThat(GenerationStageGraphCodec.sha256(graph())).isEqualTo("8373d039d72a8da44581c45f520e3b9bce59d1bcd4d488bb139a0ef388b00e80");
        assertThat(GenerationStageGraphCodec.decode(expected, GenerationStageGraphCodec.sha256(graph()))).isEqualTo(graph());
    }

    @Test
    void registrationKeepsPersistedBytesAndDigest() throws Exception {
        byte[] expected = golden("exchange-registration.json");
        assertThat(GenerationExchangeEvidenceCodec.encode(registration())).isEqualTo(expected);
        assertThat(GenerationExchangeEvidenceCodec.sha256(registration())).isEqualTo("426efd918bd7cd87eb29db74efbbc3fba906b754b7a6ef8ae328268d358d15cd");
        assertThat(GenerationExchangeEvidenceCodec.decodeRegistration(expected, GenerationExchangeEvidenceCodec.sha256(registration()))).isEqualTo(registration());
    }

    @Test
    void finalizationKeepsPersistedBytesAndDigest() throws Exception {
        byte[] expected = golden("exchange-finalization.json");
        assertThat(GenerationExchangeEvidenceCodec.encode(finalization())).isEqualTo(expected);
        assertThat(GenerationExchangeEvidenceCodec.sha256(finalization())).isEqualTo("46a7d1f615466499981917adc591450164b80c31ac6dead9b7fbbd3697a39d1e");
        assertThat(GenerationExchangeEvidenceCodec.decodeFinalization(expected, GenerationExchangeEvidenceCodec.sha256(finalization()))).isEqualTo(finalization());
    }

    @Test
    void graphEncodingCannotProduceAnUnreadablePayload() {
        var terminal = GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, GenerationStageTemplate.of(new byte[]{2}, List.of(), List.of()),
                List.of(), List.of("x".repeat(16 * 1024 * 1024)), true);
        var oversized = new GenerationStageGraph(2, GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(terminal), List.of(), List.of());
        assertThatThrownBy(() -> GenerationStageGraphCodec.encode(oversized)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size limit");
    }

    @Test
    void registrationEncodingEnforcesTheSameLimitAsReplay() {
        var oversized = registrationWithSnapshot("x".repeat(16 * 1024 * 1024));
        assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.encode(oversized)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size limit");
    }

    @Test
    void finalizationEncodingCountsBase64ExpansionAndEnvelope() {
        var r = registration();
        byte[] response = new byte[12 * 1024 * 1024];
        var oversized = new GenerationExchangeFinalization(r.exchangeId(), GenerationExchangeEvidenceCodec.sha256(r), r.shuffleId(), 7,
                r.registeredOwner(), 2, r.producerSetSha256(), finalization().partitions(), 10, 2,
                Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
        assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.encode(oversized)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size limit");
    }

    @Test
    void registrationExactlyAtTheByteLimitRemainsReadable() {
        int overhead = GenerationExchangeEvidenceCodec.encode(registrationWithSnapshot("x")).length - 1;
        var boundary = registrationWithSnapshot("x".repeat(16 * 1024 * 1024 - overhead));
        byte[] bytes = GenerationExchangeEvidenceCodec.encode(boundary);
        assertThat(bytes).hasSize(16 * 1024 * 1024);
        assertThat(GenerationExchangeEvidenceCodec.decodeRegistration(bytes, DigestUtils.sha256Hex(bytes))).isEqualTo(boundary);
    }

    @Test
    void submissionFactoryRejectsOversizedGraphBeforeReturningASpecification() {
        var graph = GenerationStageGraphFactory.forArtifactPages(new byte[12 * 1024 * 1024], new byte[]{1}, List.of(), List.of());
        var table = new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "schema", "items");
        var scope = org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(table, java.util.Set.of("id"), table);
        assertThatThrownBy(() -> org.pangrova.generation.assembly.GenerationDispatchSpecFactory.create(
                "job", graph, "tenant", "principal", scope, new GenerationDispatchSpec.ResourceGrant(1, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("size limit");
    }

    @Test
    void correctlyHashedNullExtraFieldsAndTrailingValuesCannotBecomeReplayEvidence() throws Exception {
        rejectNoncanonical("stage-graph-v2.json", GenerationStageGraphCodec::decode);
        rejectNoncanonical("exchange-registration.json", GenerationExchangeEvidenceCodec::decodeRegistration);
        rejectNoncanonical("exchange-finalization.json", GenerationExchangeEvidenceCodec::decodeFinalization);
    }

    private static void rejectNoncanonical(String fixture, BiFunction<byte[], String, ?> decode) throws Exception {
        String original = new String(golden(fixture), StandardCharsets.UTF_8);
        for (String changed : List.of("null", original + " {}", original + "\n",
                original.substring(0, original.length() - 1) + ",\"unrecognized\":true}")) {
            byte[] bytes = changed.getBytes(StandardCharsets.UTF_8);
            assertThatThrownBy(() -> decode.apply(bytes, DigestUtils.sha256Hex(bytes)))
                    .as(fixture).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static GenerationExchangeRegistration registrationWithSnapshot(String snapshot) {
        var r = registration();
        return new GenerationExchangeRegistration(r.exchangeId(), r.executionId(), r.shuffleId(), r.producerStageId(),
                r.planEpoch(), r.graphSha256(), r.stagePlanSha256(), snapshot, r.producerCount(),
                r.producerSetSha256(), r.partitionCount(), r.endpoints(), r.commitAddress(), r.registeredOwner());
    }

    private static byte[] golden(String name) throws Exception {
        try (var stream = GenerationEncodingCompatibilityTest.class.getResourceAsStream("/artifact/encoding/" + name)) {
            assertThat(stream).isNotNull();
            return stream.readAllBytes();
        }
    }
}
