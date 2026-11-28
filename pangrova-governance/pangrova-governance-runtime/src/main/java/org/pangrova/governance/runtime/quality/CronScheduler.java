package org.pangrova.governance.runtime.quality;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.pangrova.governance.api.audit.GovernanceAuditTrail;
import org.pangrova.governance.api.enums.OperationType;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Schedules periodic data quality checks using standard cron expressions.
 *
 * <p>The {@code schedule} field on {@link DataQualityRule} is interpreted as a
 * UNIX cron expression (5 fields: minute hour day-of-month month day-of-week).
 * The scheduler calculates the delay until the next execution time and reschedules
 * after each run.</p>
 *
 * <p><strong>Leader election:</strong> When running in a multi-coordinator deployment,
 * a {@link BooleanSupplier} leader check is evaluated before each scheduled execution.
 * Only the leader coordinator actually runs the quality check; non-leader nodes skip
 * with a DEBUG log. This gates admission; durable occurrence fencing is required for exactly-once execution.</p>
 *
 * <p>One monitor protects schedule replacement, cancellation and callback rearming.
 * Callbacks run on a single {@link ScheduledExecutorService} outside that monitor. The underlying executor
 * is created via {@link ManagedExecutorFactory} which sets
 * {@code setRemoveOnCancelPolicy(true)} to prevent memory leaks on cancellation.</p>
 */
@ThreadSafe
public class CronScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CronScheduler.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 5000;

    private final GovernanceStatementHandler handler;
    private final BooleanSupplier leaderCheck;
    private final GovernanceAuditTrail auditTrail;
    private final ScheduledExecutorService executor;
    private final CronParser cronParser;
    private final Map<Long, ScheduledRule> scheduledRules = new HashMap<>();
    private boolean closed;

    private static final class ScheduledRule {
        final DataQualityRule rule;
        ScheduledFuture<?> future;

        ScheduledRule(DataQualityRule rule) {
            this.rule = rule;
        }
    }

    /**
     * Creates a scheduler that always executes (single-coordinator mode).
     *
     * @param handler executes quality checks against catalog tables
     */
    public CronScheduler(GovernanceStatementHandler handler) {
        this(handler, () -> true, null);
    }

    /**
     * Creates a scheduler with leader election support.
     * Scheduled checks only execute when {@code leaderCheck} returns {@code true}.
     *
     * @param handler     executes quality checks against catalog tables
     * @param leaderCheck returns true if this node is the leader; false to skip execution
     */
    public CronScheduler(GovernanceStatementHandler handler, BooleanSupplier leaderCheck) {
        this(handler, leaderCheck, null);
    }

    /**
     * Creates a scheduler with leader election and audit trail persistence.
     * Each scheduled execution is recorded in the audit trail for compliance traceability.
     *
     * @param handler     executes quality checks against catalog tables
     * @param leaderCheck returns true if this node is the leader; false to skip execution
     * @param auditTrail  optional audit trail for recording executions (nullable)
     */
    public CronScheduler(GovernanceStatementHandler handler, BooleanSupplier leaderCheck,
                         GovernanceAuditTrail auditTrail) {
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
        this.leaderCheck = Objects.requireNonNull(leaderCheck, "leaderCheck must not be null");
        this.auditTrail = auditTrail;
        var scheduledExecutor = (java.util.concurrent.ScheduledThreadPoolExecutor)
                ManagedExecutorFactory.newSingleThreadScheduledExecutor("cron-scheduler");
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.executor = scheduledExecutor;
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    /**
     * Schedules a quality rule for periodic execution.
     * The rule's schedule field is interpreted as a UNIX cron expression
     * (e.g. {@code 0 * * * *} for every hour, {@code *&#47;5 * * * *} for every 5 minutes).
     *
     * @param rule the quality rule to schedule
     */
    public synchronized void scheduleRule(DataQualityRule rule) {
        if (closed) throw new IllegalStateException("Quality scheduler is closed");
        if (!rule.enabled() || rule.schedule() == null || rule.schedule().isBlank()) {
            cancelRule(rule.ruleId());
            return;
        }
        long delay = computeNextDelayMs(rule.schedule());
        var entry = new ScheduledRule(rule);
        arm(entry, delay);
        var previous = scheduledRules.put(rule.ruleId(), entry);
        if (previous != null) previous.future.cancel(false);
        LOG.info("action=schedule_rule, ruleName={}, cronExpression={}, initialDelayMs={}",
                rule.ruleName(), rule.schedule(), delay);
    }

    /** Cancels future occurrences. An already admitted call may finish. */
    public synchronized void cancelRule(long ruleId) {
        var entry = scheduledRules.remove(ruleId);
        if (entry != null) entry.future.cancel(false);
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            scheduledRules.values().forEach(entry -> entry.future.cancel(false));
            scheduledRules.clear();
        }
        ExecutorDrain.shutdownAndAwait(executor, SHUTDOWN_TIMEOUT_MS);
    }

    private void arm(ScheduledRule entry, long delayMs) {
        entry.future = executor.schedule(() -> ExecutorDrain.runOwned(executor, () -> runOccurrence(entry)),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private void runOccurrence(ScheduledRule entry) {
        long id = entry.rule.ruleId();
        synchronized (this) {
            if (scheduledRules.get(id) != entry) return;
        }
        try {
            executeCheck(entry.rule);
        } finally {
            synchronized (this) {
                if (scheduledRules.get(id) == entry) {
                    arm(entry, computeNextDelayMs(entry.rule.schedule()));
                }
            }
        }
    }

    private void executeCheck(DataQualityRule rule) {
        try {
            if (!leaderCheck.getAsBoolean()) {
                LOG.debug("event=scheduled_check_skip reason=not_leader ruleName={}", rule.ruleName());
                return;
            }
            var result = handler.handleRunRule(rule.ruleId());
            if (result.isEmpty()) {
                recordExecution(rule, "SKIPPED", "Rule no longer exists or is disabled");
            } else if (result.orElseThrow().passed()) {
                recordExecution(rule, "SUCCESS", null);
            } else {
                recordExecution(rule, "FAILED", result.orElseThrow().errorMessage());
            }
        } catch (Exception e) {
            LOG.warn("action=scheduled_check_failed, ruleName={}, error={}",
                rule.ruleName(), e.getMessage());
            recordExecution(rule, "FAILED", e.getMessage());
        }
    }

    private void recordExecution(DataQualityRule rule, String outcome, String errorMessage) {
        if (auditTrail == null) {
            return;
        }
        try {
            String resourcePath = rule.catalogName() + "." + rule.schemaName() + "." + rule.tableName();
            Map<String, String> afterState = Map.of(
                    "ruleName", rule.ruleName(),
                    "schedule", rule.schedule() != null ? rule.schedule() : "",
                    "outcome", outcome);
            String details = errorMessage != null
                    ? "Scheduled quality check failed: " + errorMessage
                    : "Scheduled quality check completed successfully";
            auditTrail.record(
                    "cron-scheduler",
                    OperationType.QUALITY_CHECK_EXECUTION,
                    resourcePath,
                    Map.of(),
                    afterState,
                    details);
        } catch (Exception e) {
            LOG.debug("event=audit_record_failed ruleName={} error={}",
                    rule.ruleName(), e.getMessage());
        }
    }

    /**
     * Executes a quality check for the given rule, respecting leader election.
     * Package-private for testability — allows direct invocation without waiting
     * for the scheduled executor timer.
     */
    void runCheck(DataQualityRule rule) {
        executeCheck(rule);
    }

    /**
     * Computes the delay in milliseconds until the next execution time for the given
     * cron expression. Uses cron-utils to parse the expression and calculate the next
     * execution time from now.
     *
     * @param cronExpression a UNIX cron expression (5 fields)
     * @return a positive delay to the next requested occurrence
     * @throws IllegalArgumentException if the schedule is invalid or has no next occurrence
     */
    long computeNextDelayMs(String cronExpression) {
        Cron cron = cronParser.parse(cronExpression.trim());
        cron.validate();
        Duration delay = ExecutionTime.forCron(cron).timeToNextExecution(ZonedDateTime.now())
                .orElseThrow(() -> new IllegalArgumentException("Quality schedule has no next occurrence"));
        return Math.max(1L, delay.toMillis());
    }
}
