package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.planner.enums.StagePlanMode;

import org.pangrova.execution.task.StageDispatchRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;
import java.util.HexFormat;
import org.pangrova.coordinator.query.stage.ShuffleInputLayout.Consumer;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShufflePartitionPublication;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.Partition;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange;

/** Freezes an admitted producer cohort before binding individual consumer partitions. */
final class ExchangeSourceBuilder {
    private static final long MIN_TICKET_VALIDITY_MS = 30_000L;

    private ExchangeSourceBuilder() {}

    @FunctionalInterface
    interface PartitionInput {
        ExchangeInputBinding bind(int partitionId, WorkerInfo localConsumer);
    }

    record StageInputs(List<PartitionInput> inputs) {
        private static final StageInputs EMPTY = new StageInputs(List.of());

        StageInputs {
            inputs = List.copyOf(inputs);
        }

        List<ExchangeInputBinding> bind(int partitionId, WorkerInfo localConsumer) {
            return inputs.isEmpty() ? List.of()
                    : inputs.stream().map(input -> input.bind(partitionId, localConsumer)).toList();
        }
    }

    static StageInputs prepare(StageNode stage, StageDAG dag, List<StageDispatchRecord> allDispatched) {
        List<Integer> upstreamIdList = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstreamIdList.isEmpty()) {
            return StageInputs.EMPTY;
        }
        List<PartitionInput> inputs = new ArrayList<>();
        Set<Integer> upstreamIds = Set.copyOf(upstreamIdList);
        Stage consumer = dag.findStage(stage.stageId());
        List<Consumer> layout = consumer == null ? List.of() : consumer.shuffleInputs().consumers();
        for (StageEdge edge : dag.incomingEdges(stage.stageId())) {
            if (!upstreamIds.contains(edge.upstreamStageId())) {
                continue;
            }
            Stage upstream = dag.findStage(edge.upstreamStageId());
            if (upstream != null && upstream.shuffleHandle() != null) {
                inputs.add(externalInput(upstream.publishedShuffle(), edge, layout));
            } else {
                if (!layout.isEmpty()) {
                    throw new IllegalStateException("Adaptive consumer requires its published Shuffle inputs");
                }
                if (stage.stagePlan().mode() == StagePlanMode.DURABLE_REPLAY) {
                    throw new IllegalStateException("Durable exchange has no persisted external shuffle allocation: "
                            + edge.exchangeId());
                }
                List<StageDispatchRecord> producers = allDispatched.stream()
                        .filter(record -> record.stageId() == edge.upstreamStageId())
                        .toList();
                if (producers.isEmpty()) {
                    throw new IllegalStateException("No producer tasks for exchange " + edge.exchangeId());
                }
                inputs.add((partition, worker) -> directInput(producers, edge, partition, worker));
            }
        }
        return new StageInputs(inputs);
    }

    private static ExchangeInputBinding directInput(
            List<StageDispatchRecord> producers,
            StageEdge edge,
            int partitionId, org.pangrova.coordinator.worker.WorkerInfo localConsumer) {
        DirectExchangeInput.Builder direct = DirectExchangeInput.newBuilder();
        for (StageDispatchRecord producer : producers) {
            if (localConsumer != null) requireLocalProducer(producer, localConsumer);
            direct.addSources(directSource(producer, edge, partitionId));
        }
        return ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-input-" + edge.exchangeId())
                .setExchangeId(edge.exchangeId())
                .setExpectedSourcePartitions(producers.size())
                .setNoMoreSources(true)
                .setReadMode(localConsumer == null ? protocolReadMode(edge)
                        : org.pangrova.protocol.v2.ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING)
                .setDirect(direct)
                .build();
    }

    private static void requireLocalProducer(StageDispatchRecord record,
            org.pangrova.coordinator.worker.WorkerInfo consumer) {
        if (!consumer.workerId().equals(record.result().workerId()) || record.request() == null
                || consumer.workerExecutionIdentity() == null) {
            throw new IllegalStateException("Local shuffle producer does not belong to the selected Worker");
        }
        var attempt = record.request().taskRequest().getAttempt();
        var identity = attempt.getWorkerExecutionIdentity();
        if (attempt.getAttemptId() != 0
                || !identity.equals(consumer.workerExecutionIdentity().toProto())) {
            throw new IllegalStateException("Local shuffle producer incarnation or attempt changed");
        }
    }

    private static ExchangePartitionSource directSource(
            StageDispatchRecord rec,
            StageEdge edge,
            int partitionId) {
        var endpoint = rec.result().shuffleEndpoint();
        if (endpoint == null) {
            throw new IllegalStateException(
                    "Producer did not return a Native shuffle endpoint: task="
                            + rec.result().taskId() + " exchange=" + edge.exchangeId());
        }
        var producerRequest = rec.request();
        if (producerRequest == null || !producerRequest.taskRequest().hasAttempt()) {
            throw new IllegalStateException(
                    "Producer is missing fenced task identity: task="
                            + rec.result().taskId());
        }
        var attempt = producerRequest.taskRequest().getAttempt();
        if (partitionId < 0 || partitionId >= endpoint.getPartitionTicketsCount()) {
            throw new IllegalStateException("Producer is missing shuffle ticket for partition " + partitionId);
        }
        var ticket = endpoint.getPartitionTickets(partitionId);
        if (ticket.getExpiresAtEpochMs() <= System.currentTimeMillis() + MIN_TICKET_VALIDITY_MS) {
            throw new IllegalStateException(
                    "Producer shuffle ticket expires before safe consumer bind: task="
                            + rec.result().taskId() + " exchange=" + edge.exchangeId());
        }
        return ExchangePartitionSource.newBuilder()
                .setSourceTaskId(rec.result().taskId())
                .setSourceStageId(edge.upstreamStageId())
                .setSourcePartitionId(attempt.getPartitionId())
                .setOutputPartitionId(partitionId)
                .setPlanEpoch(attempt.getPlanEpoch())
                .setAttemptId(attempt.getAttemptId())
                .setFencingToken(attempt.getFencingToken())
                .setHost(endpoint.getHost())
                .setPort(endpoint.getPort())
                .setTlsIdentity(endpoint.getTlsIdentity())
                .setSignedTicket(ticket.getSignedTicket())
                .setTicketExpiresAtEpochMs(ticket.getExpiresAtEpochMs())
                .build();
    }

    private static PartitionInput externalInput(Stage.PublishedShuffle published, StageEdge edge,
            List<Consumer> layout) {
        var receipt = published.receipt();
        var handle = published.handle();
        handle.validatePartitionCoverage(edge.partitionCount());
        ExternalShuffleInput.Builder external = ExternalShuffleInput.newBuilder()
                .setShuffleId(receipt.publication().allocationId())
                .setPartitionCount(edge.partitionCount())
                .setPlanEpoch(receipt.publication().generation())
                .setExecutionOwner(published.owner())
                .setExpectedProducerPartitions(receipt.producerCount())
                .setExpectedProducerSetChecksum(org.pangrova.protocol.v2.Checksum
                        .newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm
                                .ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(
                                HexFormat.of().parseHex(receipt.producerSetSha256())))
                        .build())
                .setCommitAddress(org.pangrova.protocol.v2.ShuffleServiceAddress
                        .newBuilder()
                        .setHost(handle.commitAddress().host())
                        .setPort(handle.commitAddress().port())
                        .setTlsIdentity(handle.commitAddress().tlsIdentity()))
                .setReadCapability(org.pangrova.protocol.v2.ShuffleAccessCapability
                        .newBuilder()
                        .setToken(ByteString.copyFrom(handle.readAccess().token()))
                        .setExpiresAtEpochMs(handle.readAccess().expiresAtEpochMs()));
        handle.endpoints().forEach(endpoint -> external.addEndpoints(
                ShuffleServiceEndpoint.newBuilder()
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                        .setPartitionStart(endpoint.partitionStart())
                        .setPartitionEnd(endpoint.partitionEnd())
                        .setTlsIdentity(endpoint.tlsIdentity())));
        var shared = ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-input-" + edge.exchangeId())
                .setExchangeId(edge.exchangeId())
                .setExpectedSourcePartitions(1)
                .setNoMoreSources(true)
                .setReadMode(protocolReadMode(edge))
                .setExternal(external)
                .build();
        if (layout.isEmpty()) {
            return (partitionId, worker) -> shared.toBuilder().setExternal(shared.getExternal().toBuilder()
                    .setSourcePartitionStart(partitionId)
                    .addPublications(wholePartition(receipt.partitions().get(partitionId)))).build();
        }
        var whole = new HashMap<Integer, ShufflePartitionPublication>();
        var ranges = new HashMap<ReadRange, ShufflePartitionPublication>();
        var bound = new ArrayList<ExchangeInputBinding>(layout.size());
        for (var consumer : layout) {
            var group = consumer.partitions();
            var binding = shared.getExternal().toBuilder().setSourcePartitionStart(group.startInclusive());
            var range = consumer.ranges().get(edge.exchangeId());
            if (range != null) {
                binding.addPublications(ranges.computeIfAbsent(range, ExchangeSourceBuilder::certifiedRange));
            } else {
                for (int id = group.startInclusive(); id < group.endExclusive(); id++) {
                    binding.addPublications(whole.computeIfAbsent(
                            id, key -> wholePartition(receipt.partitions().get(key))));
                }
            }
            bound.add(shared.toBuilder().setExternal(binding).build());
        }
        List<ExchangeInputBinding> frozen = List.copyOf(bound);
        return (partitionId, worker) -> frozen.get(partitionId);
    }

    private static ShufflePartitionPublication wholePartition(Partition partition) {
        if (partition == null) throw new IllegalArgumentException("Partition outside published Shuffle");
        return ShufflePartitionPublication.newBuilder().setLastSequence(partition.lastSequence())
                .setTotalBytes(partition.bytes()).setTotalRows(partition.rows())
                .setManifestSha256(ByteString.copyFrom(HexFormat.of().parseHex(partition.manifestSha256()))).build();
    }

    private static ShufflePartitionPublication certifiedRange(ReadRange range) {
        return ShufflePartitionPublication.newBuilder().setStartSequence(range.startSequence())
                .setLastSequence(range.endSequenceExclusive() - 1)
                .setTotalBytes(range.bytes()).setTotalRows(range.rows())
                .setManifestSha256(ByteString.copyFrom(HexFormat.of().parseHex(range.manifestSha256()))).build();
    }

    private static org.pangrova.protocol.v2.ExchangeReadMode protocolReadMode(
            StageEdge edge) {
        return switch (edge.readMode()) {
            case STREAMING -> org.pangrova.protocol.v2.ExchangeReadMode
                    .EXCHANGE_READ_MODE_STREAMING;
            case BLOCKING -> org.pangrova.protocol.v2.ExchangeReadMode
                    .EXCHANGE_READ_MODE_BLOCKING;
        };
    }
}
