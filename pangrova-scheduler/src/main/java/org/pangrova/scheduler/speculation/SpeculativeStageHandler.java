package org.pangrova.scheduler.speculation;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.pangrova.scheduler.api.placement.TaskDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Speculative execution handler owned by the scheduler.
 * Periodically detects straggler tasks and launches speculative copies
 * on different workers ("avoid original worker" placement strategy).
 *
 * <p>Gated by {@link SpeculationConfig#enabled()}. When disabled,
 * {@link #evaluateStragglers} is a no-op.</p>
 */
public final class SpeculativeStageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SpeculativeStageHandler.class);

    private final SpeculationConfig config;
    private final StragglerDetector detector;
    private final SpeculativeTaskLauncher launcher;
    private final SpeculativeTaskRegistry taskRegistry;

    /** Creates a handler wiring speculation config, straggler detection and task launching. */
    public SpeculativeStageHandler(SpeculationConfig config,
                                   StragglerDetector detector,
                                   SpeculativeTaskLauncher launcher,
                                   SpeculativeTaskRegistry taskRegistry) {
        this.config = config != null ? config : SpeculationConfig.DISABLED;
        this.detector = detector;
        this.launcher = launcher;
        this.taskRegistry = checkNotNull(taskRegistry, "taskRegistry must not be null");
    }

    /**
     * Evaluates straggler detection for a running stage and launches speculative copies.
     *
     * @param queryId the query identifier
     * @param stageId the stage identifier (as string)
     */
    public void evaluateStragglers(String queryId, String stageId) {
        if (!config.enabled() || detector == null || launcher == null) {
            return;
        }
        try {
            detectAndLaunch(queryId, stageId);
        } catch (Exception e) {
            LOG.warn("event=speculation_evaluation_error queryId={} stageId={} error={}",
                    queryId, stageId, e.getMessage(), e);
        }
    }

    public boolean isEnabled() {
        return config.enabled();
    }

    private void detectAndLaunch(String queryId, String stageId) {
        Set<String> existingSpeculative = launcher.getOriginalTasksWithCopies();
        Instant now = Instant.now();
        List<TaskDuration> snapshots =
                taskRegistry.getTaskDurationSnapshots(queryId, stageId, existingSpeculative, now)
                        .stream()
                        .filter(snapshot -> !launcher.isSpeculative(snapshot.taskId()))
                        .toList();

        List<String> stragglers = detector.detectStragglers(snapshots);
        for (String stragglerTaskId : stragglers) {
            launchSpeculativeCopy(queryId, stragglerTaskId);
        }
    }

    private void launchSpeculativeCopy(String queryId, String stragglerTaskId) {
        String originalWorkerId = taskRegistry.getWorkerForTask(stragglerTaskId);
        String excludeWorkerId = originalWorkerId != null ? originalWorkerId : "";
        String specTaskId = launcher.launchSpeculative(stragglerTaskId, excludeWorkerId, queryId);
        if (specTaskId == null) {
            LOG.info("event=speculative_launch_skipped queryId={} originalTaskId={}"
                    + " reason=no_dispatched_copy", queryId, stragglerTaskId);
            return;
        }
        LOG.info("event=speculative_launch queryId={} originalTaskId={} "
                + "speculativeTaskId={} excludeWorkerId={}",
                queryId, stragglerTaskId, specTaskId, excludeWorkerId);
    }
}
