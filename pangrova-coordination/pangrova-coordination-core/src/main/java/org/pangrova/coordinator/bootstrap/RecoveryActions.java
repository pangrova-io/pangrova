package org.pangrova.coordinator.bootstrap;

import org.pangrova.operations.recovery.RecoveryPlaybookFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers standard recovery actions into {@link RecoveryPlaybookFramework}.
 * Each action handles a specific failure scenario during coordinator operation.
 */
final class RecoveryActions {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryActions.class);

    static final String WORKER_DEAD = "WORKER_DEAD";
    static final String TASK_TIMEOUT = "TASK_TIMEOUT";
    static final String OOM_KILLED = "OOM_KILLED";

    private RecoveryActions() {}

    static void registerAll(RecoveryPlaybookFramework framework) {
        framework.register(WORKER_DEAD, ctx -> {
            LOG.warn("event=recovery_worker_dead action=mark_dead_reassign_tasks message={}", ctx.message());
        });
        framework.register(TASK_TIMEOUT, ctx -> {
            LOG.warn("event=recovery_task_timeout action=cancel_task_fail_query message={}", ctx.message());
        });
        framework.register(OOM_KILLED, ctx -> {
            LOG.error("event=recovery_oom_killed action=fail_task_resource_error message={}", ctx.message());
        });
    }
}
