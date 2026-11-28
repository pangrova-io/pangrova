package org.pangrova.query.profiling;

import org.pangrova.query.profiling.enums.DispatchPhase;
import org.pangrova.query.profiling.enums.PreparationOutcome;
import org.pangrova.query.profiling.enums.RuntimeFilterOutcome;

import java.util.List;

/** Supplies the latest measured counters for each retained Native task attempt. */
public interface OperatorStatsSource {

    /** Exact attempt telemetry for the query, or an empty list when absent. */
    List<Attempt> attemptStats(String queryId);

    /** Retained dispatch topology and Coordinator observations, if available on this process. */
    default ExecutionGraph executionGraph(String queryId) {
        return null;
    }

    record ExecutionGraph(List<Stage> stages, List<Exchange> exchanges, List<StageTransition> transitions,
            List<SelectedAttempt> selectedAttempts, List<DownstreamPreparation> downstreamPreparations,
            List<StageDispatchWork> stageDispatchWork, List<RuntimeFilterWait> runtimeFilterWaits) {
        public ExecutionGraph {
            stages = List.copyOf(stages);
            exchanges = List.copyOf(exchanges);
            transitions = List.copyOf(transitions);
            selectedAttempts = List.copyOf(selectedAttempts);
            downstreamPreparations = List.copyOf(downstreamPreparations);
            stageDispatchWork = List.copyOf(stageDispatchWork);
            runtimeFilterWaits = List.copyOf(runtimeFilterWaits);
        }
    }

    record Stage(int stageId, String taskType, int plannedParallelism) {}

    /** Physical success accepted by one exact logical slot in this stage generation. */
    record SelectedAttempt(int stageId, int generation, int partitionId, long planEpoch, int attemptId,
            String logicalTaskId, String taskId) {}

    record Exchange(String exchangeId, int producerStageId, int consumerStageId, String shuffleType,
            String readMode, int plannedPartitions) {}

    /** The offset is relative to the Coordinator's registration of this graph. */
    record StageTransition(int stageId, int generation, String previous, String state, long elapsedNanos) {}

    /** One submission to the live read preparation executor, including coalesced wakeups. */
    record DownstreamPreparation(long submittedNanos, Long startedNanos, Long finishedNanos,
            PreparationOutcome outcome) {}

    /** A completed interval measured on the graph's Coordinator, with an item count where applicable. */
    record StageDispatchWork(int stageId, int generation, DispatchPhase phase, long startedNanos,
            long finishedNanos, Integer itemCount, boolean successful) {}

    /** Outcome of a cross-stage build gate used before enumerating its probe. */
    record RuntimeFilterWait(int buildStageId, int probeStageId, int probeGeneration,
            long startedNanos, long finishedNanos, RuntimeFilterOutcome outcome, int rangeCount) {}

    /** Coordinates and counters from the latest retained report for one exact attempt. */
    record Attempt(
            int stageId,
            int partitionId,
            long planEpoch,
            int attemptId,
            String taskId,
            String workerId,
            String reportState,
            long reportSequence,
            long elapsedTimeMs,
            long cpuTimeMs,
            long backpressureWaitMs,
            List<Metric> operators,
            ObjectReads nativeObjectReads,
            ResultDelivery nativeResultDelivery) {
        public Attempt {
            operators = List.copyOf(operators);
        }
    }

    /** Native adapter work and S3 SDK transfer observations for one attempt. */
    record ObjectReads(long requestedBytes, long returnedBytes, long payloadCalls,
            long metadataCalls, long failedCalls, long s3SdkAttempts, long s3SdkRetries,
            long s3ResponseBodyBytes) {}

    /** Monotonic offsets from opening the exact Native Flight reader. */
    record ResultDelivery(long payloadBatches, Long firstPayloadNanos,
            long producerEofNanos, long transportCompleteNanos) {}

    /** One operator in the named attempt. Wait counters accumulate across its Velox drivers. */
    record Metric(
            String operatorType,
            String planNodeId,
            long inputRows,
            long outputRows,
            long inputBytes,
            long outputBytes,
            long cpuNanos,
            long wallNanos,
            long peakMemoryBytes,
            long spillBytes,
            long blockedNanos,
            long waitForProducerNanos,
            long waitForConsumerNanos,
            long waitForSplitNanos,
            long waitForConnectorNanos) {
    }
}
