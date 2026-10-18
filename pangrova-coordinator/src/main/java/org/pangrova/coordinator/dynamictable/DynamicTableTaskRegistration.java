package org.pangrova.coordinator.dynamictable;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;

import java.util.Objects;

/** Registers the production materialization handlers in one metadata factory. */
public final class DynamicTableTaskRegistration {

    private DynamicTableTaskRegistration() {}

    /** Wires the exact production DDL and refresh handlers into a metadata factory. */
    public static void register(MetadataExecutionFactory factory,
            JdbcDynamicTablePublicationStore publications, DdlExecutor tableDdl,
            NativeDmlCompileFunction dmlDispatch) {
        Objects.requireNonNull(factory, "factory");
        Objects.requireNonNull(publications, "publications");
        var materialization = new DynamicTableMaterializationService(
                publications, new CtasTask(tableDdl, dmlDispatch));
        factory.registerTask(CreateDynamicTableStatement.class, materialization::create);
        factory.registerTask(AlterDynamicTableStatement.class, materialization::alter);
        factory.registerTask(DropDynamicTableStatement.class, materialization::drop);
        factory.registerTask(ShowDynamicTablesStatement.class, materialization::show);
        factory.registerTask(DescribeDynamicTableStatement.class, materialization::describe);
        factory.registerTask(CreateContinuousQueryStatement.class, (statement, context) -> {
            throw new UnsupportedOperationException(
                    "Continuous Query materialization is not implemented");
        });
    }
}
