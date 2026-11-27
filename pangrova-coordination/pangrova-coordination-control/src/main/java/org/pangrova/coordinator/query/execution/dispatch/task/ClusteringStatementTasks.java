package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.api.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.api.ast.statement.ShowClusteringStatusStatement;

import java.util.Map;

/** Registers the metadata tasks that own SQL clustering. */
public final class ClusteringStatementTasks {
    private ClusteringStatementTasks() {}

    public static void register(Map<Class<? extends Statement>, MetadataTask<?>> tasks,
            ControlledTableMaintenance maintenance) {
        tasks.put(SetClusteringOrderStatement.class, new SetClusteringOrderTask(maintenance::setClusteringOrder));
        tasks.put(ReclusterTableStatement.class, new ReclusterTableTask(maintenance::recluster));
        tasks.put(ShowClusteringStatusStatement.class, new ShowClusteringStatusTask(maintenance::showClusteringStatus));
    }
}
