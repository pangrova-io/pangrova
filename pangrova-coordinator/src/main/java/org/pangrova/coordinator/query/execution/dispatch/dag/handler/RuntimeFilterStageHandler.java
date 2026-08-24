package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.DynamicFilterBinder;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterGenerator;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Stage-completion hook that generates runtime filters from build-side data
 * and propagates them to probe-side tasks via RPC.
 *
 * <p>When a build-side stage finishes, this handler:
 * <ol>
 *   <li>Collects build-side join key values via {@link BuildSideCollector}</li>
 *   <li>Generates a runtime filter using {@link RuntimeFilterGenerator}</li>
 *   <li>Registers the filter in {@link RuntimeFilterRegistry}</li>
 *   <li>Propagates the filter to running probe-side tasks via
 *       {@link TaskDispatcher#injectDynamicFilter}</li>
 * </ol>
 *
 * <p>Thread-safe: all mutable state is in concurrent collections.</p>
 */
public final class RuntimeFilterStageHandler implements StageLifecycleHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeFilterStageHandler.class);

    private final RuntimeFilterGenerator generator;
    private final RuntimeFilterRegistry registry;
    private final TaskDispatcher dispatcher;
    private final CoordinatorTaskTracker taskTracker;
    private final BuildSideCollector buildSideCollector;

    /**
     * Per-query mapping of build-side stageId to probe-side stageId and column info.
     * Populated via {@link #registerFilterTarget} when runtime filter descriptors
     * are available during query planning.
     */
    private final ConcurrentHashMap<String, FilterTarget> filterTargets = new ConcurrentHashMap<>();

    public RuntimeFilterStageHandler(RuntimeFilterGenerator generator,
                                     RuntimeFilterRegistry registry,
                                     TaskDispatcher dispatcher,
                                     CoordinatorTaskTracker taskTracker,
                                     BuildSideCollector buildSideCollector) {
        this.generator = checkNotNull(generator, "generator must not be null");
        this.registry = checkNotNull(registry, "registry must not be null");
        this.dispatcher = checkNotNull(dispatcher, "dispatcher must not be null");
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
        this.buildSideCollector = checkNotNull(buildSideCollector, "buildSideCollector must not be null");
    }

    /**
     * Registers a filter target mapping: when the build-side stage finishes,
     * generate a filter and propagate it to the probe-side stage.
     *
     * @param buildStageId    the build-side stage identifier
     * @param probeStageId    the probe-side stage identifier
     * @param buildColumn     the join key column on the build side
     * @param probeColumn     the join key column on the probe side
     * @param buildColumnType the declared type of the build column
     */
    public void registerFilterTarget(String buildStageId, String probeStageId,
                                     String buildColumn, String probeColumn,
                                     org.pangrova.spi.connector.data.type.PangrovaType buildColumnType) {
        filterTargets.put(buildStageId, new FilterTarget(probeStageId, buildColumn, probeColumn, buildColumnType));
        LOG.debug("event=runtime_filter_target_registered buildStageId={} probeStageId={} "
                        + "buildColumn={} probeColumn={}",
                buildStageId, probeStageId, buildColumn, probeColumn);
    }

    @Override
    public void onScheduling(StageLifecycleEvent event) {
        // No action needed during scheduling — targets are registered externally.
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        Stage finished = event.stage();
        String stageId = String.valueOf(finished.stageId());
        String queryId = event.queryId();

        FilterTarget target = filterTargets.get(stageId);
        if (target == null) {
            return; // Not a build-side stage with a registered filter target
        }

        List<? extends Comparable<?>> buildValues =
                buildSideCollector.collectBuildSideValues(queryId, stageId, target.buildColumn());
        if (buildValues == null || buildValues.isEmpty()) {
            LOG.debug("event=runtime_filter_skip_empty queryId={} buildStageId={}", queryId, stageId);
            return;
        }

        Optional<RuntimeFilter> filterOpt = generator.generate(target.probeColumn(), buildValues,
                target.buildColumnType());
        if (filterOpt.isEmpty()) {
            LOG.debug("event=runtime_filter_generation_empty queryId={} buildStageId={}",
                    queryId, stageId);
            return;
        }

        RuntimeFilter filter = filterOpt.get();
        // F-AQE-02: Namespace registry key by queryId to prevent cross-query collisions
        // when concurrent queries share the same RuntimeFilterRegistry singleton.
        String registryKey = queryId + ":" + target.probeStageId();
        registry.register(registryKey, filter);
        LOG.info("event=runtime_filter_generated queryId={} buildStageId={} probeStageId={} "
                        + "filterType={} column={}",
                queryId, stageId, target.probeStageId(), filter.type(), target.probeColumn());

        propagateToProbeStage(queryId, target, filter, event.dag());
    }

    private void propagateToProbeStage(String queryId, FilterTarget target,
                                       RuntimeFilter filter, StageDAG dag) {
        Stage probeStage = dag.findStage(Integer.parseInt(target.probeStageId()));
        if (probeStage == null) {
            LOG.warn("event=runtime_filter_probe_not_found queryId={} probeStageId={}",
                    queryId, target.probeStageId());
            return;
        }

        DynamicFilterPayload payload = DynamicFilterBinder.toPayload(target.probeStageId(), filter);
        byte[] filterProto = payload.filterData();
        String planNodeId = "scan_" + target.probeColumn();

        for (String taskId : probeStage.taskIds()) {
            String workerId = taskTracker.getWorkerForTask(taskId);
            if (workerId == null) {
                LOG.debug("event=runtime_filter_inject_skip queryId={} taskId={} "
                        + "reason=worker_not_found", queryId, taskId);
                continue;
            }
            LOG.info("event=runtime_filter_injected queryId={} taskId={} probeStageId={} "
                            + "workerId={}",
                    queryId, taskId, target.probeStageId(), workerId);
            dispatcher.injectDynamicFilter(taskId, planNodeId, filterProto, workerId);
        }
    }

    /**
     * Collects build-side join key values after a build stage completes.
     * Implementations may read from shuffle output, task result buffers,
     * or in-memory aggregation depending on the execution model.
     */
    public interface BuildSideCollector {

        /**
         * Collects the distinct values of the build-side join key column
         * produced by the completed build stage.
         *
         * @param queryId      the query identifier
         * @param stageId      the completed build-side stage identifier
         * @param buildColumn  the join key column name
         * @return list of build-side values, or empty if unavailable
         */
        List<? extends Comparable<?>> collectBuildSideValues(
                String queryId, String stageId, String buildColumn);
    }

    /**
     * Immutable target descriptor for a runtime filter propagation path.
     */
    record FilterTarget(String probeStageId, String buildColumn, String probeColumn,
                        org.pangrova.spi.connector.data.type.PangrovaType buildColumnType) {

        FilterTarget {
            checkNotNull(probeStageId, "probeStageId must not be null");
            checkNotNull(buildColumn, "buildColumn must not be null");
            checkNotNull(probeColumn, "probeColumn must not be null");
            checkNotNull(buildColumnType, "buildColumnType must not be null");
        }
    }
}
