package org.pangrova.metadata.artifact;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.metadata.artifact.build.ArtifactExchangeFinalization;
import org.pangrova.metadata.artifact.build.ArtifactExchangeRegistration;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;
import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.contract.ArtifactDependency;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataJsonMapper;
import org.pangrova.security.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual value ownership contracts only; these values are never offered as Native execution evidence. */
class MetadataCollectionOwnershipTest {
    @Test
    void tableOwnsAllFourCollectionsAndNestedColumnTags() throws Exception {
        var tags = new HashMap<>(Map.of("classification", "public"));
        var column = new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", tags);
        var columns = new ArrayList<>(List.of(column));
        var partitions = new HashMap<>(Map.of("id", "identity"));
        var order = new ArrayList<>(List.of("id"));
        var properties = new HashMap<>(Map.of("format", "parquet"));
        var table = new TableMetadata("catalog", "schema", "table", columns, partitions,
                order, properties, Instant.EPOCH, Instant.EPOCH);
        byte[] canonical = MetadataJsonMapper.writeValueAsBytes(table);
        String digest = DigestUtils.sha256Hex(canonical);

        tags.clear();
        columns.clear();
        partitions.clear();
        order.clear();
        properties.clear();

        assertThat(table.columns()).containsExactly(column);
        assertThat(table.columns().get(0).tags()).containsExactlyEntriesOf(Map.of("classification", "public"));
        assertThat(table.partitionSpec()).containsExactlyEntriesOf(Map.of("id", "identity"));
        assertThat(table.sortOrder()).containsExactly("id");
        assertThat(table.properties()).containsExactlyEntriesOf(Map.of("format", "parquet"));
        assertThatThrownBy(() -> table.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.partitionSpec().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.sortOrder().set(0, "other")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.properties().put("format", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.columns().get(0).tags().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(MetadataJsonMapper.writeValueAsBytes(table)).containsExactly(canonical);
        assertThat(DigestUtils.sha256Hex(MetadataJsonMapper.writeValueAsBytes(table))).isEqualTo(digest);
    }

    @Test
    void graphOwnsItsThreeCollectionsAndNestedStageAndExchangeCollections() {
        var sourceKeys = new ArrayList<>(List.of("catalog.schema.source"));
        var outputs = new ArrayList<>(List.of("id"));
        var source = ArtifactStageGraph.StagePlan.of(1, 1, ArtifactBuildPageKind.RELATIONAL_COMPUTE,
                bytes("source-plan"), ArtifactStageTemplate.of(new byte[0], List.of(), List.of()),
                sourceKeys, outputs, false);
        var terminal = ArtifactStageGraph.StagePlan.of(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION,
                bytes("terminal-plan"), ArtifactStageTemplate.of(bytes("bindings"), List.of(), List.of()),
                List.of(), List.of("id"), true);
        var keys = new ArrayList<>(List.of(0));
        var edge = new ArtifactStageGraph.Exchange("edge", 1, 0,
                ArtifactStageGraph.Exchange.Distribution.HASH, keys, 1, "BATCH_SERDE_ARROW_BATCH", true);
        var model = new ModelSnapshot("model", "incarnation", 1, "deployment", digest("model"));
        var stages = new ArrayList<>(List.of(source, terminal));
        var edges = new ArrayList<>(List.of(edge));
        var models = new ArrayList<>(List.of(model));
        var graph = new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT, stages, edges, models);
        byte[] canonical = graph.canonicalBytes();
        String digest = graph.sha256();

        sourceKeys.clear();
        outputs.clear();
        keys.clear();
        stages.clear();
        edges.clear();
        models.clear();

        assertThat(graph.stages()).containsExactly(terminal, source);
        assertThat(graph.exchanges()).containsExactly(edge);
        assertThat(graph.modelSnapshots()).containsExactly(model);
        assertThat(source.sourceKeys()).containsExactly("catalog.schema.source");
        assertThat(source.outputNames()).containsExactly("id");
        assertThat(edge.distributionKeys()).containsExactly(0);
        assertThatThrownBy(() -> graph.stages().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.exchanges().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.modelSnapshots().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> source.sourceKeys().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> source.outputNames().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> edge.distributionKeys().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(graph.canonicalBytes()).containsExactly(canonical);
        assertThat(graph.sha256()).isEqualTo(digest);
        assertThat(ArtifactStageGraph.decode(canonical, digest)).isEqualTo(graph);
    }

    @Test
    void templateOwnsPublicationReadsAndDecodedByteExports() throws Exception {
        var read = new ArtifactStageTemplate.PublicationRead("artifact", "generation", "source", "41",
                digest("manifest"));
        var reads = new ArrayList<>(List.of(read));
        byte[] bindings = bytes("bindings");
        byte[] resultType = bytes("result-type");
        var types = new ArrayList<>(List.of(resultType));
        var template = ArtifactStageTemplate.of(bindings, types, reads);
        byte[] canonical = MetadataJsonMapper.writeValueAsBytes(template);
        String digest = DigestUtils.sha256Hex(canonical);

        bindings[0] ^= 1;
        resultType[0] ^= 1;
        types.clear();
        reads.clear();
        template.bindingBytes()[0] ^= 1;
        template.resultTypeBytes().get(0)[0] ^= 1;

        assertThat(template.publicationReads()).containsExactly(read);
        assertThat(template.bindingBytes()).containsExactly(bytes("bindings"));
        assertThat(template.resultTypeBytes().get(0)).containsExactly(bytes("result-type"));
        assertThatThrownBy(() -> template.publicationReads().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> template.resultTypesBase64().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(MetadataJsonMapper.writeValueAsBytes(template)).containsExactly(canonical);
        assertThat(DigestUtils.sha256Hex(MetadataJsonMapper.writeValueAsBytes(template))).isEqualTo(digest);
    }

    @Test
    void dependencySetOwnsItsCanonicalCollection() throws Exception {
        var dependency = new ArtifactDependency("SOURCE_SNAPSHOT", "catalog.schema.source", "41");
        var dependencies = new ArrayList<>(List.of(dependency));
        var set = ArtifactDependencySet.of(dependencies);
        byte[] canonical = MetadataJsonMapper.writeValueAsBytes(set);
        String digest = set.sha256();

        dependencies.clear();

        assertThat(set.dependencies()).containsExactly(dependency);
        assertThatThrownBy(() -> set.dependencies().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(MetadataJsonMapper.writeValueAsBytes(set)).containsExactly(canonical);
        assertThat(set.sha256()).isEqualTo(digest);
        assertThat(ArtifactDependencySet.of(set.dependencies())).isEqualTo(set);
    }

    @Test
    void exchangeValueCollectionsOwnTheirInputAndPreserveCanonicalIdentity() {
        var address = new ArtifactExchangeRegistration.Address("store.internal", 9080, "store.internal");
        var endpoint = new ArtifactExchangeRegistration.Endpoint(address, 0, 1);
        var endpoints = new ArrayList<>(List.of(endpoint));
        var registration = registration(endpoints);
        byte[] allocationBytes = registration.canonicalBytes();
        String allocationDigest = registration.sha256();
        var partition = new ArtifactExchangeFinalization.Partition(0, 0, 0, -1, digest("manifest"));
        var partitions = new ArrayList<>(List.of(partition));
        var value = finalization(registration, partitions);
        byte[] responseBytes = value.canonicalBytes();
        String responseDigest = value.sha256();

        endpoints.clear();
        partitions.clear();
        value.responseBytes()[0] ^= 1;

        assertThat(registration.endpoints()).containsExactly(endpoint);
        assertThat(value.partitions()).containsExactly(partition);
        assertThatThrownBy(() -> registration.endpoints().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> value.partitions().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(registration.canonicalBytes()).containsExactly(allocationBytes);
        assertThat(registration.sha256()).isEqualTo(allocationDigest);
        assertThat(value.canonicalBytes()).containsExactly(responseBytes);
        assertThat(value.sha256()).isEqualTo(responseDigest);
        assertThat(value.responseBytes()).containsExactly(allocationBytes);
    }

    @Test
    void nullableTableCollectionsBecomeImmutableEmptyValues() {
        var table = new TableMetadata("catalog", "schema", "table", null, null, null, null, null, null);
        assertThat(table.columns()).isEmpty();
        assertThat(table.partitionSpec()).isEmpty();
        assertThat(table.sortOrder()).isEmpty();
        assertThat(table.properties()).isEmpty();
        assertThat(table.createdAt()).isNull();
        assertThat(table.updatedAt()).isNull();
        assertThatThrownBy(() -> table.columns().add(new ColumnMetadata("id", ColumnType.of("INT"), true, null, null)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.partitionSpec().put("id", "identity"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.sortOrder().add("id")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.properties().put("format", "parquet"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(new ColumnMetadata("id", ColumnType.of("INT"), true, null, null).tags()).isEmpty();
    }

    @Test
    void requiredArtifactCollectionsRejectNullRatherThanPublishingMutableOrPartialValues() {
        var registration = registration(List.of(new ArtifactExchangeRegistration.Endpoint(
                new ArtifactExchangeRegistration.Address("store.internal", 9080, "store.internal"), 0, 1)));
        assertThatThrownBy(() -> registration(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> finalization(registration, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                null, List.of(), List.of())).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(), null, List.of())).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(), List.of(), null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ArtifactStageTemplate.of(new byte[0], List.of(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ArtifactDependencySet.of(null)).isInstanceOf(NullPointerException.class);
        var columns = new ArrayList<ColumnMetadata>();
        columns.add(null);
        assertThatThrownBy(() -> new TableMetadata("catalog", "schema", "table", columns,
                Map.of(), List.of(), Map.of(), null, null)).isInstanceOf(NullPointerException.class);
    }

    private static ArtifactExchangeRegistration registration(List<ArtifactExchangeRegistration.Endpoint> endpoints) {
        return new ArtifactExchangeRegistration("edge", "execution", "shuffle", 1, 1, digest("graph"),
                digest("plan"), "41", 1, digest("producers"), 1, endpoints,
                new ArtifactExchangeRegistration.Address("store.internal", 9080, "store.internal"),
                new ArtifactExecutionOwner("process", "incarnation", 1));
    }

    private static ArtifactExchangeFinalization finalization(ArtifactExchangeRegistration registration,
            List<ArtifactExchangeFinalization.Partition> partitions) {
        byte[] bytes = registration.canonicalBytes();
        return new ArtifactExchangeFinalization(registration.exchangeId(), registration.sha256(),
                registration.shuffleId(), registration.planEpoch(), registration.registeredOwner(),
                registration.producerCount(), registration.producerSetSha256(), partitions, 0, 0,
                Base64.getEncoder().encodeToString(bytes), DigestUtils.sha256Hex(bytes));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String digest(String value) {
        return DigestUtils.sha256Hex(bytes(value));
    }
}
