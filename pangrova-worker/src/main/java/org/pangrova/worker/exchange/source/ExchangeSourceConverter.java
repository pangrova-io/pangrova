package org.pangrova.worker.exchange.source;

import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.exchange.DirectExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.pangrova.shuffle.exchange.ExchangeInput;
import org.pangrova.shuffle.exchange.ExchangeInputTransport;
import org.pangrova.shuffle.exchange.ExternalShuffleTransport;

import java.util.List;
import java.util.HexFormat;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;

/** Converts canonical exchange bindings to task-scoped shuffle inputs. */
public final class ExchangeSourceConverter {

    private ExchangeSourceConverter() {}

    /** Converts every protocol binding to one immutable shuffle input. */
    public static List<ExchangeInput> toExchangeInputs(
            List<ExchangeInputBinding> bindings,
            TaskAttemptId consumerAttempt,
            long deadlineEpochMs) {
        if (bindings == null || bindings.isEmpty()) {
            return List.of();
        }
        return bindings.stream().map(binding -> {
            return new ExchangeInput(
                    binding.getPlanNodeId(), binding.getExchangeId(),
                    binding.getExpectedSourcePartitions(),
                    transport(binding, consumerAttempt, deadlineEpochMs));
        }).toList();
    }

    private static ExchangeInputTransport transport(
            ExchangeInputBinding binding,
            TaskAttemptId consumerAttempt,
            long deadlineEpochMs) {
        return switch (binding.getTransportCase()) {
            case DIRECT -> new DirectExchangeTransport(
                    binding.getDirect().getSourcesList().stream()
                            .map(ExchangeSourceConverter::toEndpoint)
                            .toList());
            case EXTERNAL -> external(
                    binding.getExternal(), consumerAttempt, deadlineEpochMs);
            case TRANSPORT_NOT_SET -> throw new IllegalArgumentException(
                    "exchange input transport is required: " + binding.getExchangeId());
        };
    }

    private static ExternalShuffleTransport external(
            ExternalShuffleInput external,
            TaskAttemptId consumerAttempt,
            long deadlineEpochMs) {
        if (external.getDeadlineEpochMs() != deadlineEpochMs) {
            throw new IllegalArgumentException(
                    "external shuffle deadline does not match task deadline");
        }
        var owner = external.getExecutionOwner();
        var access = external.getReadCapability();
        if (external.getExpectedProducerSetChecksum().getAlgorithm()
                != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || external.getExpectedProducerSetChecksum().getDigest().size() != 32) {
            throw new IllegalArgumentException(
                    "external shuffle producer-set checksum is invalid");
        }
        List<ShuffleServerEndpoint> endpoints = external.getEndpointsList().stream()
                .map(endpoint -> new ShuffleServerEndpoint(
                        endpoint.getHost(), endpoint.getPort(),
                        endpoint.getPartitionStart(), endpoint.getPartitionEnd(),
                        endpoint.getTlsIdentity()))
                .toList();
        var publication = external.getPublication();
        if (!CanonicalTaskRequests.validPartitionPublication(publication)) {
            throw new IllegalArgumentException("Invalid external shuffle partition publication");
        }
        return new ExternalShuffleTransport(
                external.getShuffleId(), endpoints,
                new ShuffleServiceAddress(
                        external.getCommitAddress().getHost(),
                        external.getCommitAddress().getPort(),
                        external.getCommitAddress().getTlsIdentity()),
                external.getOutputPartitionId(), external.getPartitionCount(),
                external.getPlanEpoch(),
                new ShuffleExecutionOwner(
                        owner.getOwnerId(), owner.getOwnerIncarnation(),
                        owner.getFencingToken()),
                org.pangrova.shuffle.nativewire.NativeExchangeAttempts.decode(consumerAttempt),
                external.getDeadlineEpochMs(),
                new ShuffleAccess(
                        access.getToken().toByteArray(), access.getExpiresAtEpochMs()),
                external.getExpectedProducerPartitions(),
                external.getExpectedProducerSetChecksum().getDigest().toByteArray(),
                new ShuffleCommitReceipt.Partition(
                        publication.getLastSequence(),
                        publication.getTotalBytes(), publication.getTotalRows(),
                        HexFormat.of().formatHex(publication.getManifestSha256().toByteArray())));
    }

    private static ExchangeEndpoint toEndpoint(ExchangePartitionSource source) {
        return new ExchangeEndpoint(
                source.getSourceTaskId(), source.getSourceStageId(),
                source.getSourcePartitionId(), source.getOutputPartitionId(),
                source.getPlanEpoch(), source.getAttemptId(), source.getFencingToken(),
                source.getHost(), source.getPort(), source.getTlsIdentity(),
                source.getSignedTicket().toByteArray(),
                source.getTicketExpiresAtEpochMs(),
                source.getExpectedRows(), source.getExpectedBytes());
    }
}
