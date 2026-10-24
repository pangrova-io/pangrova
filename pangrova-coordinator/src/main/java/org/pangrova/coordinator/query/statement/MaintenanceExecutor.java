package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.control.MaintenanceOperation;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Executes table maintenance statements: COMPACT, VACUUM, OPTIMIZE.
 *
 * <p>Every statement goes through {@link ControlledTableMaintenance}. There is no
 * fallback no-op: a statement that cannot prove ownership, policy, identity, and
 * a stable version fails explicitly instead of reporting success for work that
 * never happened.</p>
 */
public final class MaintenanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceExecutor.class);

    private final ControlledTableMaintenance maintenance;

    public MaintenanceExecutor(ControlledTableMaintenance maintenance) {
        this.maintenance = Objects.requireNonNull(maintenance,
                "maintenance; maintenance without ownership enforcement is not supported");
    }

    /** Executes a table compaction statement. */
    public List<TypedRow> executeCompact(CompactStatement stmt,
            CoordinatorSessionContext context) {
        LOG.info("event=compact_table table={}", String.join(".", stmt.tableName().parts()));
        return maintenance.execute(MaintenanceOperation.COMPACT, stmt.tableName(), context);
    }

    /** Executes a table vacuum statement. */
    public List<TypedRow> executeVacuum(VacuumStatement stmt,
            CoordinatorSessionContext context) {
        LOG.info("event=vacuum_table table={}", String.join(".", stmt.tableName().parts()));
        return maintenance.execute(MaintenanceOperation.VACUUM, stmt.tableName(), context);
    }

    /** Executes a table optimization statement. */
    public List<TypedRow> executeOptimize(OptimizeStatement stmt,
            CoordinatorSessionContext context) {
        LOG.info("event=optimize_table table={}", String.join(".", stmt.tableName().parts()));
        return maintenance.execute(MaintenanceOperation.OPTIMIZE, stmt.tableName(), context);
    }
}
