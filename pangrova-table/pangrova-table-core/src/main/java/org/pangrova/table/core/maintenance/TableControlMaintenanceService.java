package org.pangrova.table.core.maintenance;

import org.pangrova.table.api.ownership.OwnershipMode;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.maintenance.MaintenanceOperation;
import org.pangrova.table.api.maintenance.MaintenanceRunState;
import org.pangrova.persistence.table.api.maintenance.TableControlMaintenanceStore;
import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;
import org.pangrova.security.api.error.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Authorizes maintenance and records its history.
 *
 * <p>Policy is keyed by {@code table_id}. A LINKED table starts with no enabled
 * operations; adopting it never grants rewrite rights implicitly. Every run is
 * persisted so an operator can see what was attempted, with which version, and
 * why it was skipped or failed.</p>
 */
public final class TableControlMaintenanceService {

    private static final Logger LOG =
            LoggerFactory.getLogger(TableControlMaintenanceService.class);

    public static final String PROP_OPERATIONS = "maintenance_operations";
    public static final String PROP_RETAIN_LAST = "retain_last";
    public static final String PROP_ORPHAN_FILE_AGE_HOURS = "orphan_file_age_hours";
    public static final String PROP_SNAPSHOT_RETENTION_HOURS = "snapshot_retention_hours";
    public static final String PROP_ACTIVE_WRITER_GUARD = "active_writer_guard";

    private static final Set<String> ADOPTION_KEYS = Set.of(
            PROP_OPERATIONS, PROP_RETAIN_LAST, PROP_ORPHAN_FILE_AGE_HOURS,
            PROP_SNAPSHOT_RETENTION_HOURS, PROP_ACTIVE_WRITER_GUARD);

    private final TableControlMaintenanceStore store;
    private final Clock clock;

    public TableControlMaintenanceService(TableControlMaintenanceStore store) {
        this(store, Clock.systemUTC());
    }

    public TableControlMaintenanceService(TableControlMaintenanceStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public Optional<TableMaintenancePolicy> findPolicy(UUID tableId) {
        return store.findPolicy(tableId);
    }

    /** Returns the persisted policy, or the ownership-appropriate defaults. */
    public TableMaintenancePolicy policyFor(TableControlRecord record) {
        return store.findPolicy(record.tableId())
                .orElseGet(() -> TableMaintenanceDefaults.defaultsFor(record, clock.instant()));
    }

    public void savePolicy(TableMaintenancePolicy policy) {
        store.savePolicy(policy);
    }

    /**
     * Applies {@code ALTER TABLE ... ADOPT WITH (...)} properties.
     *
     * <p>Unknown keys and unknown operation names are rejected instead of being
     * ignored: silently dropping a misspelled authorization would leave an
     * operator believing maintenance is enabled when it is not.</p>
     */
    public TableMaintenancePolicy applyAdoptionProperties(
            TableControlRecord record, Map<String, String> properties, String actor) {
        TableMaintenancePolicy policy = parseAdoptionProperties(
                record.ownershipMode(), record.tableId(), properties, actor);
        if (properties == null || properties.isEmpty()) {
            return policy;
        }
        store.savePolicy(policy);
        LOG.info("event=table_maintenance_policy_saved table={} tableId={} operations={} "
                        + "retainLast={} orphanFileAge={}",
                record.qualifiedName(), record.tableId(), policy.enabledOperations(),
                policy.retainLast(), policy.orphanFileAge());
        return policy;
    }

    /**
     * Validates properties before any ownership state is written. A rejected
     * ADOPT must not leave a control record behind for a statement that failed.
     */
    public void validateAdoptionProperties(
            OwnershipMode mode, Map<String, String> properties) {
        parseAdoptionProperties(mode, new UUID(0L, 0L), properties, "");
    }

    private TableMaintenancePolicy parseAdoptionProperties(OwnershipMode mode, UUID tableId,
            Map<String, String> properties, String actor) {
        TableMaintenancePolicy defaults = TableMaintenanceDefaults.defaultsFor(
                mode, tableId, clock.instant());
        if (properties == null || properties.isEmpty()) {
            return defaults;
        }
        for (String key : properties.keySet()) {
            if (!ADOPTION_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
                throw new AccessDeniedException("ADOPT rejected: unknown maintenance property '"
                        + key + "'; supported properties are " + ADOPTION_KEYS);
            }
        }
        Set<MaintenanceOperation> operations = parseOperations(
                value(properties, PROP_OPERATIONS), defaults.enabledOperations());
        int retainLast = parsePositiveInt(
                value(properties, PROP_RETAIN_LAST), defaults.retainLast(), PROP_RETAIN_LAST);
        Duration orphanAge = parseHours(value(properties, PROP_ORPHAN_FILE_AGE_HOURS),
                defaults.orphanFileAge(), PROP_ORPHAN_FILE_AGE_HOURS);
        Duration snapshotRetention = parseHours(
                value(properties, PROP_SNAPSHOT_RETENTION_HOURS),
                defaults.snapshotRetention(), PROP_SNAPSHOT_RETENTION_HOURS);
        boolean guard = parseBoolean(value(properties, PROP_ACTIVE_WRITER_GUARD),
                defaults.requireStableExternalVersion(), PROP_ACTIVE_WRITER_GUARD);
        if (mode == OwnershipMode.LINKED && !guard) {
            throw new AccessDeniedException("ADOPT rejected: an active-writer guard cannot be "
                    + "disabled for LINKED tables; concurrent external writers must be detected "
                    + "before maintenance");
        }
        return new TableMaintenancePolicy(tableId, operations, retainLast, orphanAge,
                snapshotRetention, guard, actor, clock.instant());
    }

    /** Records the start of a run before any connector call is made. */
    public TableMaintenanceRun beginRun(TableControlRecord record,
            MaintenanceOperation operation, String mode, String versionBefore, String actor) {
        TableMaintenanceRun run = TableMaintenanceRun.running(
                record, operation, mode, versionBefore, actor, clock.instant());
        store.saveRun(run);
        return run;
    }

    /** Persists the terminal state of a run. */
    public TableMaintenanceRun finishRun(TableMaintenanceRun run, MaintenanceRunState state,
            String versionAfter, Map<String, String> details) {
        TableMaintenanceRun finished = run.finished(
                state, versionAfter, details, clock.instant());
        store.saveRun(finished);
        return finished;
    }

    public List<TableMaintenanceRun> listRuns(UUID tableId, int limit) {
        return store.listRuns(tableId, limit);
    }

    private static String value(Map<String, String> properties, String key) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Set<MaintenanceOperation> parseOperations(
            String raw, Set<MaintenanceOperation> fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        EnumSet<MaintenanceOperation> operations = EnumSet.noneOf(MaintenanceOperation.class);
        for (String token : raw.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            MaintenanceOperation operation = MaintenanceOperation.parse(trimmed)
                    .orElseThrow(() -> new AccessDeniedException(
                            "ADOPT rejected: unknown maintenance operation '" + trimmed + "'"));
            operations.add(operation);
        }
        return operations;
    }

    private static int parsePositiveInt(String raw, int fallback, String name) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            if (parsed < 1) {
                throw new AccessDeniedException(
                        "ADOPT rejected: " + name + " must be at least 1, got " + parsed);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new AccessDeniedException(
                    "ADOPT rejected: " + name + " must be an integer, got '" + raw + "'");
        }
    }

    private static Duration parseHours(String raw, Duration fallback, String name) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            long hours = Long.parseLong(raw.trim());
            if (hours < 1) {
                throw new AccessDeniedException(
                        "ADOPT rejected: " + name + " must be positive, got " + hours);
            }
            return Duration.ofHours(hours);
        } catch (NumberFormatException e) {
            throw new AccessDeniedException(
                    "ADOPT rejected: " + name + " must be an integer hour count, got '"
                            + raw + "'");
        }
    }

    private static boolean parseBoolean(String raw, boolean fallback, String name) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(raw.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw.trim())) {
            return false;
        }
        throw new AccessDeniedException(
                "ADOPT rejected: " + name + " must be true or false, got '" + raw + "'");
    }
}
