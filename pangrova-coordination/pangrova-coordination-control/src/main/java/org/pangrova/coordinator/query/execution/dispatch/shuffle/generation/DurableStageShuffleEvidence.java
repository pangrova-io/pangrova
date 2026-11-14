package org.pangrova.coordinator.query.execution.dispatch.shuffle.generation;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence;

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
        var edge = requireStage(registration);
        return persisted(registration.stageId()).map(saved -> {
            var allocation = new Allocation(saved.shuffleId(), saved.descriptor());
            if (!saved.equals(registrationRecord(registration, edge.exchangeId(), allocation, saved.registeredOwner()))) {
                throw new IllegalStateException("Restored shuffle allocation differs from the requested stage cohort");
            }
            return allocation;
        });
    }

    @Override
    public void registered(ShuffleRegistration registration, Allocation allocation, ShuffleExecutionOwner owner) {
        var edge = requireStage(registration);
        var issued = new GenerationExecutionOwner(owner.ownerId(), owner.ownerIncarnation(), owner.fencingToken());
        if (!issued.equals(currentOwner.get())) {
            throw new IllegalStateException("Shuffle registration has a stale owner");
        }
        var firstOwner = persisted(registration.stageId()).map(GenerationExchangeRegistration::registeredOwner)
                .orElse(issued);
        store.register(job, issued, registrationRecord(registration, edge.exchangeId(), allocation, firstOwner));
    }

    private GenerationExchangeRegistration registrationRecord(ShuffleRegistration registration, String exchangeId,
            Allocation allocation, GenerationExecutionOwner registeredOwner) {
        return new GenerationExchangeRegistration(exchangeId, job.executionId(), allocation.shuffleId(),
                registration.stageId(), registration.planEpoch(), GenerationStageGraphCodec.sha256(spec.stageGraph()),
                spec.stageGraph().requireStage(registration.stageId()).planSha256(), sourceSnapshot,
                registration.expectedProducerCount(), hex(registration.expectedProducerSetChecksum()),
                registration.numPartitions(), allocation.descriptor(), registeredOwner);
    }

    @Override
    public void finalized(ShuffleRegistration registration, Allocation observed,
            ShuffleExecutionOwner requestOwner, ShuffleCommitReceipt receipt, byte[] response) {
        requireStage(registration);
        var allocation = persisted(registration.stageId()).orElseThrow(() ->
                new IllegalStateException("Real shuffle finalization has no persisted server allocation"));
        var owner = currentOwner.get();
        var issued = new GenerationExecutionOwner(requestOwner.ownerId(), requestOwner.ownerIncarnation(),
                requestOwner.fencingToken());
        if (!issued.equals(owner) || !receipt.publication().allocationId().equals(allocation.shuffleId())
                || !observed.equals(new Allocation(allocation.shuffleId(), allocation.descriptor()))
                || receipt.publication().generation() != allocation.planEpoch()
                || !receipt.publication().queryId().equals(job.executionId())
                || receipt.publication().stageId() != registration.stageId()
                || receipt.producerCount() != allocation.producerCount()
                || !receipt.producerSetSha256().equals(allocation.producerSetSha256())) {
            throw new IllegalStateException("External finalize RPC changed allocation, owner, epoch or producer cohort");
        }
        List<GenerationExchangeFinalization.Partition> partitions = receipt.partitions().entrySet().stream()
                .map(entry -> {
                    var value = entry.getValue();
                    return new GenerationExchangeFinalization.Partition(entry.getKey(), value.bytes(),
                            value.rows(), value.lastSequence(), value.manifestSha256());
                }).toList();
        var proof = new GenerationExchangeFinalization(
                allocation.exchangeId(), GenerationExchangeEvidenceCodec.sha256(allocation), observed.shuffleId(),
                receipt.publication().generation(), owner, receipt.producerCount(), receipt.producerSetSha256(),
                partitions, partitions.stream().mapToLong(GenerationExchangeFinalization.Partition::totalBytes)
                        .reduce(0, Math::addExact),
                partitions.stream().mapToLong(GenerationExchangeFinalization.Partition::totalRows).reduce(0, Math::addExact),
                Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
        GenerationExchangeEvidenceCodec.requireRegistration(proof, allocation);
        store.finalizeExchange(job, owner, proof);
    }

    private org.pangrova.generation.api.graph.GenerationStageGraph.Exchange requireStage(
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

    private static String hex(byte[] bytes) { return HexFormat.of().formatHex(bytes); }
}
