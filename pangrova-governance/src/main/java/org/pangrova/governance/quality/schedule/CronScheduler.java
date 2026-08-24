package org.pangrova.governance.quality.schedule;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.pangrova.governance.audit.GovernanceAuditTrail;
import org.pangrova.governance.audit.OperationType;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;

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
 * with a DEBUG log. This ensures exactly-once execution per interval across the cluster.</p>
 *
 * <p>Concurrency strategy: {@link ConcurrentHashMap} for scheduled task registry;
 * {@link ScheduledExecutorService} for periodic execution. The underlying executor
 * is created via {@link ManagedExecutorFactory} which sets
 * {@code setRemoveOnCancelPolicy(true)} to prevent memory leaks on cancellation.</p>
 */
@ThreadSafe
public class CronScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CronScheduler.class);
    private static final long DEFAULT_INTERVAL_MS = 3_600_000;

    private final GovernanceStatementHandler handler;
    private final BooleanSupplier leaderCheck;
    private final GovernanceAuditTrail auditTrail;
    private final ScheduledExecutorService executor;
    private final CronParser cronParser;
    private final Map<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

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
        this.executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("cron-scheduler");
        this.cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
    }

    /**
     * Schedules a quality rule for periodic execution.
     * The rule's schedule field is interpreted as a UNIX cron expression
     * (e.g. {@code 0 * * * *} for every hour, {@code *&#47;5 * * * *} for every 5 minutes).
     *
     * @param rule the quality rule to schedule
     */
    public void scheduleRule(DataQualityRule rule) {
        if (rule.schedule() == null || rule.schedule().isBlank()) return;
        long delayMs = computeNextDelayMs(rule.schedule());
        scheduleNext(rule, delayMs);
        LOG.info("action=schedule_rule, ruleName={}, cronExpression={}, initialDelayMs={}",
                rule.ruleName(), rule.schedule(), delayMs);
    }

    /**
     * Cancels a previously scheduled rule.
     *
     * @param ruleId the rule identifier to cancel
     */
    public void cancelRule(long ruleId) {
        ScheduledFuture<?> future = scheduledTasks.remove(ruleId);
        if (future != null) {
            future.cancel(false);
        }
    }

    @Override
    public void close() {
        ResourceCloser.shutdownExecutor(executor, Duration.ofSeconds(5));
    }

    private void scheduleNext(DataQualityRule rule, long delayMs) {
        ScheduledFuture<?> future = executor.schedule(
                () -> {
                    executeCheck(rule);
                    long nextDelay = computeNextDelayMs(rule.schedule());
                    scheduleNext(rule, nextDelay);
                },
                delayMs, TimeUnit.MILLISECONDS);
        scheduledTasks.put(rule.ruleId(), future);
    }

    private void executeCheck(DataQualityRule rule) {
        if (!leaderCheck.getAsBoolean()) {
            LOG.debug("event=scheduled_check_skip reason=not_leader ruleName={}", rule.ruleName());
            return;
        }
        try {
            handler.handleRunCheck(rule.catalogName(), rule.schemaName(), rule.tableName());
            recordExecution(rule, "SUCCESS", null);
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
     * @return delay in milliseconds until next execution; defaults to 1 hour on parse failure
     */
    long computeNextDelayMs(String cronExpression) {
        try {
            Cron cron = cronParser.parse(cronExpression.trim());
            cron.validate();
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            ZonedDateTime now = ZonedDateTime.now();
            Optional<Duration> nextExecution = executionTime.timeToNextExecution(now);
            return nextExecution.map(Duration::toMillis).orElse(DEFAULT_INTERVAL_MS);
        } catch (IllegalArgumentException e) {
            LOG.warn("action=cron_parse_failed, expression={}, error={}, using default interval",
                    cronExpression, e.getMessage());
            return DEFAULT_INTERVAL_MS;
        }
    }
}
