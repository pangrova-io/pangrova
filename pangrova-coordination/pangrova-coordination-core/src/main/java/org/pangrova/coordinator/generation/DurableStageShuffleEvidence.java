package org.pangrova.coordinator.generation;

import org.pangrova.generation.protocol.GenerationExchangeEvidenceCodec;

import org.pangrova.generation.protocol.GenerationStageGraphCodec;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.generation.ShuffleLifecycleEvidence;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.spi.generation.execution.GenerationExchangeFinalization;
import org.pangrova.spi.generation.execution.GenerationExchangeRegistration;
import org.pangrova.spi.generation.execution.GenerationExecutionOwner;
import org.pangrova.persistence.api.generation.exchange.GenerationStageExchangeStore;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.core.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Persists capability-free proof from real ordinary-DAG allocation/finalize RPCs. */
public final class DurableStageShuffleEvidence implements ShuffleLifecycleEvidence {
    private final GenerationStageExchangeStore store;
    private final GenerationDispatchSpec spec;
    private final String sourceSnapshot;
    private final Supplier<GenerationExecutionOwner> currentOwner;
    private final GenerationJobKey job;

    /** Binds a durable artifact job to its real shuffle allocation and finalization store. */
    public DurableStageShuffleEvidence(GenerationStageExchangeStore store, GenerationDispatchSpec spec,
            String sourceSnapshot, Supplier<GenerationExecutionOwner> currentOwner) {
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.spec = java.util.Objects.requireNonNull(spec, "spec");
        this.sourceSnapshot = java.util.Objects.requireNonNull(sourceSnapshot, "sourceSnapshot");
        this.currentOwner = java.util.Objects.requireNonNull(currentOwner, "currentOwner");
        this.job = new GenerationJobKey(spec.tenantId(), spec.jobId());
    }

    @Override
    public Optional<Allocation> restoredAllocation(ShuffleRegistration registration) {
        requireStage(registration);
        return persisted(registration.stageId()).map(value -> new Allocation(value.shuffleId(),
                new ShuffleServiceAddress(value.commitAddress().host(), value.commitAddress().port(),
                        value.commitAddress().tlsIdentity())));
    }

    @Override
    public void registered(ShuffleRegistration registration, ShuffleHandle handle, ShuffleExecutionOwner owner) {
        var edge = requireStage(registration);
        var issued = new GenerationExecutionOwner(owner.ownerId(), owner.ownerIncarnation(), owner.fencingToken());
        if (!issued.equals(currentOwner.get())) {
            throw new IllegalStateException("Shuffle registration has a stale owner");
        }
        var allocation = new GenerationExchangeRegistration(edge.exchangeId(), job.executionId(), handle.shuffleId(),
                registration.stageId(), registration.planEpoch(), GenerationStageGraphCodec.sha256(spec.stageGraph()),
                spec.stageGraph().requireStage(registration.stageId()).planSha256(), sourceSnapshot,
                registration.expectedProducerCount(), hex(registration.expectedProducerSetChecksum()),
                registration.numPartitions(), handle.endpoints().stream().map(endpoint ->
                        new GenerationExchangeRegistration.Endpoint(new GenerationExchangeRegistration.Address(
                                endpoint.host(), endpoint.port(), endpoint.tlsIdentity()),
                                endpoint.partitionStart(), endpoint.partitionEnd())).toList(),
                new GenerationExchangeRegistration.Address(handle.commitAddress().host(), handle.commitAddress().port(),
                        handle.commitAddress().tlsIdentity()), issued);
        var old = persisted(registration.stageId());
        if (old.isPresent()) {
            var unchanged = old.orElseThrow();
            allocation = new GenerationExchangeRegistration(allocation.exchangeId(), allocation.executionId(),
                    allocation.shuffleId(), allocation.producerStageId(), allocation.planEpoch(),
                    allocation.graphSha256(),
                    allocation.stagePlanSha256(), allocation.sourceSnapshot(), allocation.producerCount(),
                    allocation.producerSetSha256(), allocation.partitionCount(), allocation.endpoints(),
                    allocation.commitAddress(), unchanged.registeredOwner());
        }
        store.register(job, issued, allocation);
    }

    @Override
    public void finalized(ShuffleRegistration registration, ShuffleHandle handle,
            FinalizeShuffleRequest request, FinalizeShuffleResponse response) {
        requireStage(registration);
        var allocation = persisted(registration.stageId()).orElseThrow(() ->
                new IllegalStateException("Real shuffle finalization has no persisted server allocation"));
        var owner = currentOwner.get();
        requireFinalizeIdentity(allocation, owner, request, response);
        byte[] bytes = response.toByteArray();
        List<GenerationExchangeFinalization.Partition> partitions = response.getPartitionsList().stream().map(value ->
                new GenerationExchangeFinalization.Partition(value.getPartitionId(), value.getTotalBytes(),
                        value.getTotalRows(), value.getCommittedLastSequence(),
                        sha(value.getOutputChecksum()))).toList();
        var proof = new GenerationExchangeFinalization(
                allocation.exchangeId(), GenerationExchangeEvidenceCodec.sha256(allocation), handle.shuffleId(),
                request.getPlanEpoch(), owner, response.getCommittedProducerCount(),
                sha(response.getProducerSetChecksum()),
                partitions, response.getTotalBytes(), response.getTotalRows(),
                Base64.getEncoder().encodeToString(bytes),
                DigestUtils.sha256Hex(bytes));
        GenerationExchangeEvidenceCodec.requireRegistration(proof, allocation);
        store.finalizeExchange(job, owner, proof);
    }

    private org.pangrova.spi.generation.execution.GenerationStageGraph.Exchange requireStage(
            ShuffleRegistration registration) {
        var edges = spec.stageGraph().exchanges().stream().filter(edge ->
                edge.upstreamStageId() == registration.stageId()).toList();
        if (edges.size() != 1 || !registration.queryId().equals(job.executionId())
                || registration.expectedProducerCount()
                        != spec.stageGraph().requireStage(registration.stageId()).parallelism()
                || registration.numPartitions() != edges.get(0).partitionCount()
                || !java.util.Arrays.equals(registration.expectedProducerSetChecksum(),
                        ShuffleProducerSetChecksum.forContiguousPartitions(
                                registration.queryId(), registration.stageId(),
                                registration.planEpoch(), registration.expectedProducerCount()))) {
            throw new IllegalArgumentException("Live shuffle request differs from the immutable durable stage cohort");
        }
        return edges.get(0);
    }

    private Optional<GenerationExchangeRegistration> persisted(int stageId) {
        return store.load(job, currentOwner.get()).stream()
                .map(GenerationStageExchangeStore.ExchangeEvidence::registration)
                .filter(value -> value.producerStageId() == stageId).findFirst();
    }

    private static void requireFinalizeIdentity(GenerationExchangeRegistration allocation,
            GenerationExecutionOwner owner, FinalizeShuffleRequest request, FinalizeShuffleResponse response) {
        var requestedOwner = request.getExecutionOwner();
        if (!request.getShuffleId().equals(allocation.shuffleId())
                || request.getPlanEpoch() != allocation.planEpoch()
                || !owner.equals(new GenerationExecutionOwner(
                        requestedOwner.getOwnerId(), requestedOwner.getOwnerIncarnation(),
                        requestedOwner.getFencingToken()))
                || request.getExpectedProducerPartitions() != allocation.producerCount()
                || !sha(request.getExpectedProducerSetChecksum()).equals(allocation.producerSetSha256())
                || response.getCommittedProducerCount() != allocation.producerCount()
                || !sha(response.getProducerSetChecksum()).equals(allocation.producerSetSha256())) {
            throw new IllegalStateException(
                    "External finalize RPC changed allocation, owner, epoch or producer cohort");
        }
    }

    private static String sha(Checksum checksum) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256 || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("External shuffle requires exact SHA-256 evidence");
        }
        return hex(checksum.getDigest().toByteArray());
    }

    private static String hex(byte[] bytes) { return HexFormat.of().formatHex(bytes); }
}
