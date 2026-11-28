package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PreparedPartition;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExchangeReadMode;

import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

import java.util.Arrays;
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
            if (inputs.size() != 1 || !inputs.get(0).hasExternal()) {
                throw new IllegalArgumentException("Consumer task requires one actually finalized external allocation");
            }
            var finalized = evidence.finalization().orElseThrow(() ->
                    new IllegalArgumentException("Consumer task requires one actually finalized external allocation"));
            var binding = inputs.get(0);
            requireBlocking(binding);
            requireInput(binding.getExternal(), prepared, evidence.registration(), finalized, access.descriptorEncoder());
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
            GenerationExchangeRegistration registration, GenerationExchangeFinalization finalized,
            ExternalShuffleDescriptorEncoder encoder) {
        if (!input.getShuffleId().equals(registration.shuffleId())
                || input.getPlanEpoch() != registration.planEpoch()
                || !input.hasSourcePartitionStart()
                || input.getSourcePartitionStart() != prepared.lease().partitionId()
                || input.getPublicationsCount() != 1
                || input.getPartitionCount() != registration.partitionCount()
                || input.getExpectedProducerPartitions() != registration.producerCount()) {
            throw new IllegalArgumentException("Consumer access changed the immutable allocation, epoch or cohort");
        }
        requireChecksum(input.getExpectedProducerSetChecksum(), registration);
        requirePublishedRange(input, finalized);
        requireConsumerTransport(input, prepared, registration, encoder);
    }

    static void requirePublishedRange(ExternalShuffleInput input, GenerationExchangeFinalization finalized) {
        int start = input.getSourcePartitionStart();
        int count = input.getPublicationsCount();
        if (!input.hasSourcePartitionStart() || start < 0 || count == 0
                || start > finalized.partitions().size() - count) {
            throw new IllegalArgumentException("Consumer range is outside the persisted Shuffle receipt");
        }
        for (int offset = 0; offset < count; offset++) {
            var expected = finalized.partitions().get(start + offset);
            var actual = input.getPublications(offset);
            if (!actual.hasLastSequence() || actual.getLastSequence() != expected.committedLastSequence()
                    || !actual.hasTotalBytes() || actual.getTotalBytes() != expected.totalBytes()
                    || !actual.hasTotalRows() || actual.getTotalRows() != expected.totalRows()
                    || !Arrays.equals(actual.getManifestSha256().toByteArray(),
                            HexFormat.of().parseHex(expected.manifestSha256()))) {
                throw new IllegalArgumentException("Consumer publication differs from the persisted Shuffle receipt");
            }
        }
    }

    private static void requireConsumerTransport(ExternalShuffleInput input, PreparedPartition prepared,
            GenerationExchangeRegistration registration, ExternalShuffleDescriptorEncoder encoder) {
        var route = input.getCommitAddress();
        var address = new ShuffleServiceAddress(route.getHost(), route.getPort(), route.getTlsIdentity());
        var endpoints = input.getEndpointsList().stream().map(endpoint -> new ShuffleServerEndpoint(endpoint.getHost(),
                endpoint.getPort(), endpoint.getPartitionStart(), endpoint.getPartitionEnd(),
                        endpoint.getTlsIdentity())).toList();
        if (!exactOwner(input.getExecutionOwner(), prepared) || encoder == null
                || !encoder.describe(input.getShuffleId(), address, endpoints, registration.partitionCount())
                        .equals(registration.descriptor())) {
            throw new IllegalArgumentException("Consumer access changed the real external service identity");
        }
    }

    private static void requireChecksum(Checksum checksum, GenerationExchangeRegistration registration) {
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
            List<org.pangrova.generation.api.graph.GenerationStageGraph.Exchange> outgoing) {
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
            org.pangrova.generation.api.graph.GenerationStageGraph.Exchange edge) {
        var registration = prepared.exchanges().stream().map(value -> value.registration())
                .filter(value -> value.exchangeId().equals(edge.exchangeId())).findFirst().orElseThrow();
        if (!access.allocation().shuffleId().equals(registration.shuffleId())
                || access.planEpoch() != registration.planEpoch()
                || access.allocation().expectedProducerCount() != registration.producerCount()
                || access.output().numPartitions() != edge.partitionCount()
                || !access.output().partitionKeyIndices().equals(edge.distributionKeys())) {
            throw new IllegalArgumentException("Producer task changed the exact persisted allocation and cohort");
        }
        requireProducerTransport(access.allocation(), registration, access.descriptorEncoder());
    }

    private static void requireProducerTransport(org.pangrova.shuffle.api.ShuffleHandle handle,
            GenerationExchangeRegistration registration, ExternalShuffleDescriptorEncoder encoder) {
        if (encoder == null || !encoder.describe(handle.shuffleId(), handle.commitAddress(), handle.endpoints(),
                registration.partitionCount()).equals(registration.descriptor())
                || !HexFormat.of().formatHex(handle.expectedProducerSetChecksum())
                        .equals(registration.producerSetSha256())) {
            throw new IllegalArgumentException("Producer task changed the persisted service or checksum identity");
        }
    }
}
