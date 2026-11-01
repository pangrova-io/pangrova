package org.pangrova.protocol.rest.copy;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SourcePreparationRequest;
import org.pangrova.protocol.v2.SourcePreparationResult;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;

import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrepareCopySourceRequestTest {
    @Test
    void roundTripsARealPreparationWithoutPlanOrNativeMemoryAndRedactsItsBearer() throws Exception {
        var request = request();
        var json = JsonMapperFactory.shared().writeValueAsBytes(request);
        var decoded = JsonMapperFactory.shared().readValue(json, PrepareCopySourceRequest.class);
        assertThat(decoded.preparation()).isEqualTo(request.preparation());
        assertThat(decoded.canonicalBytes()).isEqualTo(request.canonicalBytes());
        assertThat(decoded.sourceRead()).isEqualTo(request.sourceRead());
        assertThat(request.preparation().getResourceGrant().getNativeMemoryBytes()).isZero();
        assertThat(request.toString()).doesNotContain(request.sourceRead().nonce());
        assertThat(request.admissionSeal().getCanonicalRequestChecksum())
                .isEqualTo(ProtocolChecksums.sha256(request.canonicalBytes()));
    }

    @Test
    void malformedAuthorityWrongSubjectAndUnfundedHeapAreRejected() {
        var request = request();
        var changed = request.preparation().toBuilder();
        changed.getSecurityBuilder().setPrincipal("other");
        var wrongSubject = changed.build();
        assertThatThrownBy(() -> PrepareCopySourceRequest.from(wrongSubject, request.sourceRead()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("private authority");
        changed = request.preparation().toBuilder();
        changed.getResourceGrantBuilder().setJvmHeapBytes(0);
        var unfunded = changed.build();
        assertThatThrownBy(() -> PrepareCopySourceRequest.from(unfunded, request.sourceRead()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("JVM memory grant");
        assertThatThrownBy(() -> new PrepareCopySourceRequest("not base64", request.sourceRead()))
                .isInstanceOf(IllegalArgumentException.class);
        var unknown = request.preparation().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        assertThatThrownBy(() -> PrepareCopySourceRequest.from(unknown, request.sourceRead()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failedReceiptPreservesEnteredUsageButCannotChangeIssuanceOrExceedItsGrant() throws Exception {
        var request = request();
        var result = SourcePreparationResult.newBuilder().setAttempt(request.preparation().getAttempt())
                .setRequestSha256(ProtocolChecksums.sha256(request.canonicalBytes())).setIoJoined(true)
                .setUsage(ResourceUsage.newBuilder().setObjectStoreReadBytes(7))
                .setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_TRANSPORT)
                        .setCode("TEST_SOURCE_FAILURE").setFailedComponent("test-source").setMessage("backend request failed")).build();
        var response = PrepareCopySourceResponse.from(result);
        var decoded = JsonMapperFactory.shared().readValue(JsonMapperFactory.shared().writeValueAsBytes(response),
                PrepareCopySourceResponse.class);
        assertThat(decoded.resultFor(request).getUsage().getObjectStoreReadBytes()).isEqualTo(7);
        var excessive = PrepareCopySourceResponse.from(result.toBuilder().setUsage(
                ResourceUsage.newBuilder().setObjectStoreReadBytes(17)).build());
        assertThatThrownBy(() -> excessive.resultFor(request)).isInstanceOf(IllegalArgumentException.class);
        var wrong = PrepareCopySourceResponse.from(result.toBuilder().setRequestSha256(
                ProtocolChecksums.sha256(new byte[]{1})).build());
        assertThatThrownBy(() -> wrong.resultFor(request)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pendingReceiptNeverClaimsThatAnUnfinishedOwnerHasJoined() {
        var request = request();
        var pending = SourcePreparationResult.newBuilder().setAttempt(request.preparation().getAttempt())
                .setRequestSha256(ProtocolChecksums.sha256(request.canonicalBytes())).setUsage(ResourceUsage.getDefaultInstance())
                .setIoJoined(false).setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_DEADLINE)
                        .setCode("COPY_SOURCE_STILL_ACTIVE").setFailedComponent("test-source").setMessage("still active")).build();
        assertThat(PrepareCopySourceResponse.from(pending).resultFor(request).getIoJoined()).isFalse();
        assertThatThrownBy(() -> PrepareCopySourceResponse.from(pending.toBuilder().clearError().build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preparedProofAndPrivateBearerHaveBoundedValidatedIdentities() {
        var request = request();
        assertThatThrownBy(() -> PrepareCopySourceRequest.from(request.preparation(),
                new CopySourceReadHttp.RedeemRequest("n".repeat(44), request.sourceRead().scope())))
                .isInstanceOf(IllegalArgumentException.class);
        var source = org.pangrova.protocol.v2.PreparedFileSource.newBuilder().setSourceVersionId("version-1")
                .setManagedObjectUri("s3://managed/copy/object").setContentSha256("a".repeat(64))
                .setTenantScope("b".repeat(64)).setContentSize(5).build();
        var result = SourcePreparationResult.newBuilder().setAttempt(request.preparation().getAttempt())
                .setRequestSha256(ProtocolChecksums.sha256(request.canonicalBytes()))
                .setUsage(ResourceUsage.getDefaultInstance()).setIoJoined(true).setSource(source).build();
        assertThat(PrepareCopySourceResponse.from(result).resultFor(request)).isEqualTo(result);
        for (var invalid : java.util.List.of(source.toBuilder().setManagedObjectUri("s3://reader:secret@managed/file").build(),
                source.toBuilder().setContentSha256("bad").build(), source.toBuilder().setContentSize(-1).build(),
                source.toBuilder().setSourceVersionId("").build(), source.toBuilder().setTenantScope("").build())) {
            assertThatThrownBy(() -> PrepareCopySourceResponse.from(result.toBuilder().setSource(invalid).build()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static PrepareCopySourceRequest request() {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1, "s3://incoming/file.parquet", "s3://managed/copy",
                "a".repeat(64), "query-7", "prepare-42", "coordinator", "test", 1, 1, 4_102_444_800_000L);
        var command = SourcePreparationRequest.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder().setQueryOrJobId(scope.executionId()).setTaskId(scope.stagingAttemptId())
                        .setPlanEpoch(1).setFencingToken(1L << 32 | 1L).setControlEpoch(scope.controlEpoch())
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(scope.ownerId())
                                .setOwnerIncarnation(scope.ownerIncarnation()).setFencingToken(scope.ownerFencingToken())))
                .setSecurity(SecurityContext.newBuilder().setTenantId(scope.tenantId()).setPrincipal(scope.principal()))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId("prepare-42:grant").setCpuSlots(1)
                        .setJvmHeapBytes(1_048_576).setObjectStoreReadBytes(16).setObjectStoreWriteBytes(8).setSpillBytes(8))
                .setDeadlineEpochMs(scope.deadlineEpochMs()).setSourceCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256())))).build();
        return PrepareCopySourceRequest.from(command, new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope));
    }
}
