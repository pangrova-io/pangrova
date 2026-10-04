package org.pangrova.protocol;

import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.substrait.proto.Expression;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.BatchSerde;
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

    @Test
    void batchFormatHasOneWireImplementationAndReservesRemovedValueAndName() {
        var descriptor = BatchSerde.getDescriptor().toProto();
        assertThat(descriptor.getValueList()).extracting(value -> value.getNumber()).containsExactly(0, 2);
        assertThat(descriptor.getReservedNameList()).containsExactly("BATCH_SERDE_NATIVE_ROW");
        assertThat(descriptor.getReservedRangeList()).singleElement().satisfies(range -> {
            assertThat(range.getStart()).isEqualTo(1);
            assertThat(range.getEnd()).isEqualTo(1);
        });
        assertThat(BatchSerde.forNumber(1)).isNull();
        assertThat(BatchSerde.forNumber(2)).isEqualTo(BatchSerde.BATCH_SERDE_ARROW_BATCH);
    }

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
    void invocationBoundaryExcludesBillingLedgerAndReservesRetiredField() {
        var model = CanonicalProtocolDescriptors.descriptorSet().getFileList().stream()
                .filter(file -> file.getName().equals("pangrova/protocol/v2/model.proto"))
                .findFirst().orElseThrow();
        assertThat(model.getMessageTypeList()).extracting(message -> message.getName())
                .doesNotContain("AiUsage");
        var result = model.getMessageTypeList().stream()
                .filter(message -> message.getName().equals("AiInvocationResult"))
                .findFirst().orElseThrow();
        assertThat(result.getFieldList()).extracting(field -> field.getName())
                .doesNotContain("usage");
        assertThat(result.getFieldList()).extracting(field -> field.getNumber()).doesNotContain(4);
        assertThat(result.getReservedNameList()).contains("usage");
        assertThat(result.getReservedRangeList())
                .anyMatch(range -> range.getStart() == 4 && range.getEnd() == 5);
        assertThat(org.pangrova.protocol.v2.ResourceUsage.getDescriptor()
                .findFieldByName("peak_native_memory_bytes")).isNotNull();
    }

    @Test
    void consumerPressureUsesGenericStatsAndKeepsLiveCountSeparateFromElapsedWait() throws Exception {
        var descriptor = org.pangrova.protocol.v2.NativeTaskStats.getDescriptor();
        var field = descriptor.findFieldByName("consumer_blocked_drivers");
        assertThat(field).isNotNull();
        assertThat(field.getNumber()).isEqualTo(18);
        assertThat(field.getType()).isEqualTo(com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64);
        var original = org.pangrova.protocol.v2.NativeTaskStats.newBuilder()
                .setConsumerBlockedDrivers(2)
                .setBackpressureWaitMs(750)
                .build();
        var decoded = org.pangrova.protocol.v2.NativeTaskStats.parseFrom(original.toByteArray());
        assertThat(decoded.getConsumerBlockedDrivers()).isEqualTo(2);
        assertThat(decoded.getBackpressureWaitMs()).isEqualTo(750);
        assertThat(decoded.toBuilder().clearConsumerBlockedDrivers().build().getConsumerBlockedDrivers())
                .isZero();
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
