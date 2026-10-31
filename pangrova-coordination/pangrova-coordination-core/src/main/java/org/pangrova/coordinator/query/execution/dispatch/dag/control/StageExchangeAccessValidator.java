package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.databuild.api.execution.DataBuildExchangeRegistration;
import org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore.PreparedPartition;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExchangeReadMode;

import java.util.HexFormat;
import java.util.List;

/** Pure comparison of ephemeral external access against evidence locked on the exact partition claim connection. */
final class StageExchangeAccessValidator {
    private StageExchangeAccessValidator() {}

    static void validate(PreparedPartition prepared, DurablePartitionRequest.ExchangeAccess access) {
        var graph = prepared.authority().dispatchSpec().stageGraph();
        int stage = prepared.lease().stageId();
        var incoming = graph.exchanges().stream().filter(edge -> edge.downstreamStageId() == stage).toList();
        if (access.inputs().size() != incoming.size()) {
            throw new IllegalArgumentException("Task inputs differ from the exact persisted incoming edges");
        }
        for (var edge : incoming) {
            var inputs = access.inputs().stream()
                    .filter(input -> input.getExchangeId().equals(edge.exchangeId())).toList();
            var evidence = prepared.exchanges().stream().filter(value ->
                    value.registration().exchangeId().equals(edge.exchangeId())).findFirst().orElseThrow();
            if (inputs.size() != 1 || !inputs.get(0).hasExternal() || evidence.finalization().isEmpty()) {
                throw new IllegalArgumentException("Consumer task requires one actually finalized external allocation");
            }
            var binding = inputs.get(0);
            requireBlocking(binding);
            requireInput(inputs.get(0).getExternal(), prepared, evidence.registration());
        }
        var outgoing = graph.exchanges().stream().filter(edge -> edge.upstreamStageId() == stage).toList();
        requireOutput(prepared, access, outgoing);
    }

    private static void requireBlocking(org.pangrova.protocol.v2.ExchangeInputBinding binding) {
        if (binding.getExpectedSourcePartitions() != 1 || !binding.getNoMoreSources()
                || binding.getReadMode() != ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING) {
            throw new IllegalArgumentException("Durable consumer must use the exact complete blocking exchange");
        }
    }

    private static void requireInput(ExternalShuffleInput input, PreparedPartition prepared,
            DataBuildExchangeRegistration registration) {
        if (!input.getShuffleId().equals(registration.shuffleId())
                || input.getPlanEpoch() != registration.planEpoch()
                || input.getOutputPartitionId() != prepared.lease().partitionId()
                || input.getPartitionCount() != registration.partitionCount()
                || input.getExpectedProducerPartitions() != registration.producerCount()) {
            throw new IllegalArgumentException("Consumer access changed the immutable allocation, epoch or cohort");
        }
        requireChecksum(input.getExpectedProducerSetChecksum(), registration);
        requireConsumerTransport(input, prepared, registration);
    }

    private static void requireConsumerTransport(ExternalShuffleInput input, PreparedPartition prepared,
            DataBuildExchangeRegistration registration) {
        if (!exactOwner(input.getExecutionOwner(), prepared)
                || !protocolAddress(input.getCommitAddress()).equals(registration.commitAddress())
                || !input.getEndpointsList().stream().map(endpoint -> new DataBuildExchangeRegistration.Endpoint(
                        new DataBuildExchangeRegistration.Address(endpoint.getHost(), endpoint.getPort(),
                                endpoint.getTlsIdentity()), endpoint.getPartitionStart(), endpoint.getPartitionEnd()))
                        .toList().equals(registration.endpoints())) {
            throw new IllegalArgumentException("Consumer access changed the real external service identity");
        }
    }

    private static DataBuildExchangeRegistration.Address protocolAddress(
            org.pangrova.protocol.v2.ShuffleServiceAddress value) {
        return new DataBuildExchangeRegistration.Address(value.getHost(), value.getPort(), value.getTlsIdentity());
    }

    private static void requireChecksum(Checksum checksum, DataBuildExchangeRegistration registration) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256 || checksum.getDigest().size() != 32
                || !HexFormat.of().formatHex(checksum.getDigest().toByteArray())
                        .equals(registration.producerSetSha256())) {
            throw new IllegalArgumentException("Consumer access changed the immutable SHA-256 producer cohort");
        }
    }

    private static boolean exactOwner(ExecutionOwnerIdentity owner, PreparedPartition prepared) {
        var anchor = prepared.authority().executionAnchor();
        return owner.equals(ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken()).build());
    }

    private static void requireOutput(PreparedPartition prepared, DurablePartitionRequest.ExchangeAccess access,
            List<org.pangrova.databuild.api.execution.DataBuildStageGraph.Exchange> outgoing) {
        if (outgoing.isEmpty()) {
            if (access.output() != null || access.allocation() != null) {
                throw new IllegalArgumentException("Terminal partition cannot bind an unregistered output exchange");
            }
            return;
        }
        if (outgoing.size() != 1 || access.output() == null || access.allocation() == null) {
            throw new IllegalArgumentException("Producer requires exactly one persisted external allocation");
        }
        requireProducerAllocation(prepared, access, outgoing.get(0));
    }

    private static void requireProducerAllocation(PreparedPartition prepared,
            DurablePartitionRequest.ExchangeAccess access,
            org.pangrova.databuild.api.execution.DataBuildStageGraph.Exchange edge) {
        var registration = prepared.exchanges().stream().map(value -> value.registration())
                .filter(value -> value.exchangeId().equals(edge.exchangeId())).findFirst().orElseThrow();
        if (!access.allocation().shuffleId().equals(registration.shuffleId())
                || access.planEpoch() != registration.planEpoch()
                || access.allocation().expectedProducerCount() != registration.producerCount()
                || access.output().numPartitions() != edge.partitionCount()
                || !access.output().partitionKeyIndices().equals(edge.distributionKeys())) {
            throw new IllegalArgumentException("Producer task changed the exact persisted allocation and cohort");
        }
        requireProducerTransport(access.allocation(), registration);
    }

    private static void requireProducerTransport(org.pangrova.shuffle.api.ShuffleHandle handle,
            DataBuildExchangeRegistration registration) {
        var endpoints = handle.endpoints().stream().map(endpoint -> new DataBuildExchangeRegistration.Endpoint(
                new DataBuildExchangeRegistration.Address(endpoint.host(), endpoint.port(), endpoint.tlsIdentity()),
                endpoint.partitionStart(), endpoint.partitionEnd())).toList();
        var address = handle.commitAddress();
        if (!endpoints.equals(registration.endpoints())
                || !new DataBuildExchangeRegistration.Address(address.host(), address.port(), address.tlsIdentity())
                        .equals(registration.commitAddress())
                || !HexFormat.of().formatHex(handle.expectedProducerSetChecksum())
                        .equals(registration.producerSetSha256())) {
            throw new IllegalArgumentException("Producer task changed the persisted service or checksum identity");
        }
    }
}
