package org.pangrova.protocol;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.substrait.proto.Expression;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.RetrievalSourceDetail;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class CanonicalProtocolDescriptorsTest {

    private static final Set<String> CANONICAL_FILES = Set.of(
            "pangrova/protocol/v2/capability.proto",
            "pangrova/protocol/v2/catalog.proto",
            "pangrova/protocol/v2/common.proto",
            "pangrova/protocol/v2/connector.proto",
            "pangrova/protocol/v2/execution_binding.proto",
            "pangrova/protocol/v2/index.proto",
            "pangrova/protocol/v2/model.proto",
            "pangrova/protocol/v2/plan_extension.proto",
            "pangrova/protocol/v2/protocol_manifest.proto",
            "pangrova/protocol/v2/remote_shuffle.proto",
            "pangrova/protocol/v2/result.proto",
            "pangrova/protocol/v2/runtime.proto",
            "pangrova/protocol/v2/task.proto",
            "substrait/algebra.proto",
            "substrait/extended_expression.proto",
            "substrait/extensions/extensions.proto",
            "substrait/plan.proto",
            "substrait/type.proto",
            "google/protobuf/any.proto",
            "google/protobuf/empty.proto");

    @Test
    void descriptorSetContainsEveryCanonicalFileInStableOrder() throws Exception {
        byte[] bytes = CanonicalProtocolDescriptors.descriptorSetBytes();
        FileDescriptorSet parsed = FileDescriptorSet.parseFrom(bytes);
        assertThat(parsed.getFileList())
                .extracting(value -> value.getName())
                .isSorted()
                .containsExactlyInAnyOrderElementsOf(CANONICAL_FILES);
        assertThat(CanonicalProtocolDescriptors.descriptorSetBytes()).isEqualTo(bytes);
    }

    @Test
    void descriptorSetHashIsSha256() {
        byte[] hash = CanonicalProtocolDescriptors.sha256();
        assertThat(hash).hasSize(32);
        assertThat(CanonicalProtocolDescriptors.sha256())
                .isEqualTo(hash);
    }

    @Test
    void manifestKeepsBoundaryClosedOverCanonicalFiles() {
        Set<String> files = CanonicalProtocolDescriptors.descriptorSet().getFileList().stream()
                .map(value -> value.getName())
                .collect(Collectors.toUnmodifiableSet());
        assertThat(files).isEqualTo(CANONICAL_FILES);
    }

    @Test
    void canonicalBoundaryClosesOverGenericV2AndPinnedSubstrait() throws Exception {
        byte[] bytes = CanonicalProtocolDescriptors.descriptorSetBytes();
        FileDescriptorSet parsed = FileDescriptorSet.parseFrom(bytes);
        assertThat(parsed.getFileList())
                .extracting(value -> value.getName())
                .isSorted()
                .containsExactlyInAnyOrderElementsOf(CANONICAL_FILES);
        assertThat(CanonicalProtocolDescriptors.descriptorSetBytes()).isEqualTo(bytes);
    }

    @Test
    void v2TaskBoundaryHasNoFeatureSpecificVectorOrSearchPlane() {
        Descriptor task = TaskRequest.getDescriptor();
        assertThat(task.getFields())
                .extracting(field -> field.getName().toLowerCase())
                .noneMatch(name -> name.contains("vector") || name.contains("search"));

        List<String> boundaryTypes = CanonicalProtocolDescriptors.descriptorSet()
                .getFileList().stream()
                .filter(file -> file.getName().startsWith("pangrova/protocol/v2/"))
                .flatMap(file -> file.getMessageTypeList().stream())
                .map(message -> message.getName().toLowerCase())
                .toList();
        assertThat(boundaryTypes).noneMatch(name -> name.contains("vectorsearch"));
    }

    @Test
    void retrievalRelationCarriesTypedSubstraitExpressions() throws Exception {
        Expression query = Expression.newBuilder()
                .setLiteral(Expression.Literal.newBuilder().setString("typed query"))
                .build();
        RetrievalSourceDetail detail = RetrievalSourceDetail.newBuilder()
                .setPlanNodeId("retrieval-7")
                .setSourceId("catalog.search_service")
                .setCandidateLimit(100)
                .setInvocation(CapabilityInvocation.newBuilder()
                        .setCapability(CapabilityRef.newBuilder()
                                .setCapabilityId("retrieval.source")
                                .setSemanticVersion("2.0.0"))
                        .addArguments(NamedExpressionArgument.newBuilder()
                                .setName("query")
                                .setExpression(query)))
                .build();

        RelationDetailEnvelope envelope = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setRetrievalSource(detail)
                .build();
        RelationDetailEnvelope decoded = RelationDetailEnvelope.parseFrom(envelope.toByteArray());

        assertThat(decoded.getRetrievalSource().getInvocation()
                .getArguments(0).getExpression()).isEqualTo(query);
    }
}
