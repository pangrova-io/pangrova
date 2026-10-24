package org.pangrova.metadata.control;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One maintenance attempt against a controlled table.
 *
 * <p>The run is keyed by {@code tableId}, not by name, so history cannot be
 * attributed to a table that was later recreated under the same name. A run
 * records the version observed before execution; a LINKED run that finds the
 * version changed mid-flight is reported as skipped rather than assumed safe.</p>
 */
public record TableMaintenanceRun(
        UUID runId,
        UUID tableId,
        MaintenanceOperation operation,
        String mode,
        MaintenanceRunState state,
        String versionBefore,
        String versionAfter,
        Map<String, String> details,
        String actor,
        Instant startedAt,
        Instant finishedAt) {

    public TableMaintenanceRun {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(tableId, "tableId");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(state, "state");
        mode = mode == null || mode.isBlank() ? "MANUAL" : mode;
        versionBefore = versionBefore == null ? "" : versionBefore;
        versionAfter = versionAfter == null ? "" : versionAfter;
        details = details == null ? Map.of() : Map.copyOf(details);
        actor = actor == null ? "" : actor;
        Objects.requireNonNull(startedAt, "startedAt");
        if (state == MaintenanceRunState.RUNNING && finishedAt != null) {
            throw new IllegalArgumentException("a running maintenance run cannot have finishedAt");
        }
        if (state != MaintenanceRunState.RUNNING && finishedAt == null) {
            throw new IllegalArgumentException("a terminal maintenance run requires finishedAt");
        }
    }

    public static TableMaintenanceRun running(TableControlRecord record,
            MaintenanceOperation operation, String mode, String versionBefore,
            String actor, Instant startedAt) {
        return new TableMaintenanceRun(UUID.randomUUID(), record.tableId(), operation, mode,
                MaintenanceRunState.RUNNING, versionBefore, "", Map.of(), actor,
                startedAt, null);
    }

    public TableMaintenanceRun finished(MaintenanceRunState finalState, String newVersion,
            Map<String, String> finalDetails, Instant completedAt) {
        return new TableMaintenanceRun(runId, tableId, operation, mode, finalState,
                versionBefore, newVersion, finalDetails, actor, startedAt, completedAt);
    }
}
