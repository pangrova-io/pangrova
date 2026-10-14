package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.StagePlanAccess;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.protocol.v2.DirectExchangeInput;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;

/**
 * Builds exchange sources for downstream stages in a DAG dispatch.
 * Resolves upstream task locations and fetches splits for leaf stages.
 */
final class ExchangeSourceBuilder {
    private static final long MIN_TICKET_VALIDITY_MS = 30_000L;

    private ExchangeSourceBuilder() {}

    /**
     * Collects exchange sources from upstream dispatched tasks for the given stage
     * and consumer partition. Each consumer task receives its own partitionId so
     * hash-exchange plans with downstream parallelism N create N consumers pulling
     * partitions 0..N-1.
     *
     * @param partitionId the consumer task's partition index (0-based)
     */
    static List<ExchangeInputBinding> buildExchangeInputs(
            StageNode stage, StageDAG dag,
            List<DAGDispatchEngine.StageDispatchRecord> allDispatched,
            int partitionId) {
        List<Integer> upstreamIdList = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstreamIdList.isEmpty()) {
            return List.of();
        }
        List<ExchangeInputBinding> inputs = new ArrayList<>();
        Set<Integer> upstreamIds = Set.copyOf(upstreamIdList);
        for (StageEdge edge : dag.stageEdges()) {
            if (edge.downstreamStageId() != stage.stageId()
                    || !upstreamIds.contains(edge.upstreamStageId())) {
                continue;
            }
            List<DAGDispatchEngine.StageDispatchRecord> producers = allDispatched.stream()
                    .filter(record -> record.stageId() == edge.upstreamStageId())
                    .toList();
            Stage upstream = dag.findStage(edge.upstreamStageId());
            if (upstream != null && upstream.shuffleHandle() != null
                    && !upstream.shuffleHandle().endpoints().isEmpty()) {
                inputs.add(externalInput(upstream, edge, partitionId));
            } else {
                if (producers.isEmpty()) {
                    throw new IllegalStateException("No producer tasks for exchange " + edge.exchangeId());
                }
                if (stage.stagePlan().mode() == StagePlanAccess.Mode.DURABLE_REPLAY) {
                    throw new IllegalStateException("Durable exchange has no persisted external shuffle allocation: "
                            + edge.exchangeId());
                }
                inputs.add(directInput(producers, edge, partitionId));
            }
        }
        return List.copyOf(inputs);
    }

    /**
     * Fetches splits with file path metadata for cache affinity routing.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithFilePaths(
            StageNode stage, StageDAG dag, CatalogRegistry catalogRegistry,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        return fetchSplitsWithFilePaths(stage, dag, catalogRegistry, java.util.Map.of(), session);
    }

    /**
     * Fetches splits with snapshot overrides for time-travel queries.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithFilePaths(
            StageNode stage, StageDAG dag, CatalogRegistry catalogRegistry,
            java.util.Map<String, Long> snapshotOverrides,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (!upstreamIds.isEmpty()) {
            return List.of();
        }
        StagePlanAccess fragment = stage.stagePlan();
        List<SplitSourceDescriptor> sources = fragment.splitSources();
        List<String> outputColumns = fragment.queryOutputColumns();
        return SplitFetcher.fetchSplitsWithFilePaths(
                sources, catalogRegistry, snapshotOverrides, outputColumns, session);
    }

    /**
     * Fetches splits with index-based filtering for leaf stages.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithIndexFiltering(
            StageNode stage, StageDAG dag,
            org.pangrova.coordinator.index.IndexAwareSplitFetcher fetcher,
            java.util.Map<String, Long> snapshotOverrides,
            String queryId,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (!upstreamIds.isEmpty()) {
            return List.of();
        }
        return fetcher.fetchAndFilterSplits(
                stage.stagePlan().requireLiveFragment(), snapshotOverrides, queryId, session);
    }

    private static ExchangeInputBinding directInput(
            List<DAGDispatchEngine.StageDispatchRecord> producers,
            StageEdge edge,
            int partitionId) {
        DirectExchangeInput.Builder direct = DirectExchangeInput.newBuilder();
        for (DAGDispatchEngine.StageDispatchRecord producer : producers) {
            direct.addSources(directSource(producer, edge, partitionId));
        }
        return ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-input-" + edge.exchangeId())
                .setExchangeId(edge.exchangeId())
                .setExpectedSourcePartitions(producers.size())
                .setNoMoreSources(true)
                .setReadMode(protocolReadMode(edge))
                .setDirect(direct)
                .build();
    }

    private static ExchangePartitionSource directSource(
            DAGDispatchEngine.StageDispatchRecord rec,
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
        var ticket = endpoint.ticketFor(partitionId);
        if (ticket.expiresAtEpochMs() <= System.currentTimeMillis() + MIN_TICKET_VALIDITY_MS) {
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
                .setHost(endpoint.host())
                .setPort(endpoint.port())
                .setTlsIdentity(endpoint.tlsIdentity())
                .setSignedTicket(ByteString.copyFrom(
                        Base64.getDecoder().decode(ticket.signedTicket())))
                .setTicketExpiresAtEpochMs(ticket.expiresAtEpochMs())
                .build();
    }

    private static ExchangeInputBinding externalInput(
            Stage upstream, StageEdge edge, int partitionId) {
        if (upstream.shufflePlanEpoch() <= 0
                || upstream.shuffleExecutionOwner() == null) {
            throw new IllegalStateException(
                    "External shuffle allocation is missing producer identity: stage="
                            + upstream.stageId());
        }
        var handle = upstream.shuffleHandle();
        handle.validatePartitionCoverage(edge.partitionCount());
        ExternalShuffleInput.Builder external = ExternalShuffleInput.newBuilder()
                .setShuffleId(handle.shuffleId())
                .setOutputPartitionId(partitionId)
                .setPartitionCount(edge.partitionCount())
                .setPlanEpoch(upstream.shufflePlanEpoch())
                .setExecutionOwner(upstream.shuffleExecutionOwner())
                .setExpectedProducerPartitions(handle.expectedProducerCount())
                .setExpectedProducerSetChecksum(org.pangrova.protocol.v2.Checksum
                        .newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm
                                .ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(
                                handle.expectedProducerSetChecksum()))
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
        return ExchangeInputBinding.newBuilder()
                .setPlanNodeId("exchange-input-" + edge.exchangeId())
                .setExchangeId(edge.exchangeId())
                .setExpectedSourcePartitions(1)
                .setNoMoreSources(true)
                .setReadMode(protocolReadMode(edge))
                .setExternal(external)
                .build();
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
