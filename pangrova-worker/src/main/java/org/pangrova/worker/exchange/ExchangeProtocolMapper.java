package org.pangrova.worker.exchange;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.ShuffleServiceAddress;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;

/** Maps canonical exchange domain objects to the Native runtime protocol. */
final class ExchangeProtocolMapper {

    private ExchangeProtocolMapper() {}

    static ExchangePartitionSource directSource(ExchangeEndpoint source) {
        return ExchangePartitionSource.newBuilder()
                .setSourceTaskId(source.sourceTaskId())
                .setSourceStageId(source.sourceStageId())
                .setSourcePartitionId(source.sourcePartitionId())
                .setOutputPartitionId(source.outputPartitionId())
                .setPlanEpoch(source.planEpoch())
                .setAttemptId(source.attemptId())
                .setFencingToken(source.fencingToken())
                .setHost(source.host())
                .setPort(source.transportPort())
                .setTlsIdentity(source.tlsIdentity())
                .setSignedTicket(ByteString.copyFrom(source.signedTicket()))
                .setTicketExpiresAtEpochMs(source.ticketExpiresAtEpochMs())
                .setExpectedRows(source.expectedRows())
                .setExpectedBytes(source.expectedBytes())
                .build();
    }

    static ExternalShuffleInput externalInput(ExternalShuffleTransport input) {
        var owner = input.executionOwner();
        var access = input.readAccess();
        var commit = input.commitAddress();
        ExternalShuffleInput.Builder builder = ExternalShuffleInput.newBuilder()
                .setShuffleId(input.shuffleId())
                .setOutputPartitionId(input.outputPartitionId())
                .setPartitionCount(input.partitionCount())
                .setPlanEpoch(input.planEpoch())
                .setDeadlineEpochMs(input.deadlineEpochMs())
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(owner.ownerId())
                        .setOwnerIncarnation(owner.ownerIncarnation())
                        .setFencingToken(owner.fencingToken()))
                .setReadCapability(ShuffleAccessCapability.newBuilder()
                        .setToken(ByteString.copyFrom(access.token()))
                        .setExpiresAtEpochMs(access.expiresAtEpochMs()))
                .setCommitAddress(ShuffleServiceAddress.newBuilder()
                        .setHost(commit.host()).setPort(commit.port())
                        .setTlsIdentity(commit.tlsIdentity()))
                .setExpectedProducerPartitions(input.expectedProducerPartitions())
                .setExpectedProducerSetChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(input.expectedProducerSetChecksum())));
        input.endpoints().forEach(endpoint -> builder.addEndpoints(
                ShuffleServiceEndpoint.newBuilder()
                        .setHost(endpoint.host()).setPort(endpoint.port())
                        .setPartitionStart(endpoint.partitionStart())
                        .setPartitionEnd(endpoint.partitionEnd())
                        .setTlsIdentity(endpoint.tlsIdentity())));
        return builder.build();
    }

    static TaskAttemptId taskAttempt(ExchangeTaskAttempt attempt) {
        var owner = attempt.executionOwner();
        return TaskAttemptId.newBuilder()
                .setQueryOrJobId(attempt.queryId())
                .setStageId(attempt.stageId())
                .setPartitionId(attempt.taskPartitionId())
                .setTaskId(attempt.taskId())
                .setControlEpoch(attempt.controlEpoch())
                .setPlanEpoch(attempt.fence().planEpoch())
                .setAttemptId(attempt.fence().attemptId())
                .setFencingToken(attempt.fence().fencingToken())
                .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(owner.ownerId())
                        .setOwnerIncarnation(owner.ownerIncarnation())
                        .setFencingToken(owner.fencingToken()))
                .build();
    }
}
