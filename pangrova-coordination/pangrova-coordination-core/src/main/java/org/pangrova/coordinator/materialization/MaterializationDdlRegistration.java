package org.pangrova.coordinator.materialization;

import java.util.Objects;
import javax.sql.DataSource;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.materialization.store.jdbc.JdbcMaterializationCatalog;
import org.pangrova.materialization.store.jdbc.JdbcMaterializationRepository;
import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationDependenciesStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationGcHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationRefreshHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationsStatement;
import org.pangrova.parser.ast.statement.ShowMaterializedViewsStatement;
import org.pangrova.spi.security.AccessControl;

/** Registers the unified materialization DDL handlers in one metadata factory. */
public final class MaterializationDdlRegistration {

    private MaterializationDdlRegistration() {
    }

    /**
     * Registers the unified materialization DDL handlers.
     *
     * @param factory metadata execution factory receiving the handlers
     * @param repository durable materialization repository
     * @param catalog published-materialization read model
     * @param catalogs connector catalog registry
     * @param coordination leader election
     * @param tableDdl shared table DDL executor
     * @param dmlDispatch native DML compile function
     * @param metadataDataSource metadata data source
     * @param accessControl authorization policy
     * @return the wired kernel factory
     */
    public static MaterializationKernelFactory register(MetadataExecutionFactory factory,
            JdbcMaterializationRepository repository,
            JdbcMaterializationCatalog catalog,
            CatalogRegistry catalogs,
            HighAvailabilityCoordinationService coordination,
            DdlExecutor tableDdl,
            NativeDmlCompileFunction dmlDispatch,
            DataSource metadataDataSource,
            AccessControl accessControl) {
        Objects.requireNonNull(factory, "factory");
        MaterializationKernelFactory kernels = new MaterializationKernelFactory(
                Objects.requireNonNull(repository, "repository"),
                Objects.requireNonNull(catalog, "catalog"),
                Objects.requireNonNull(catalogs, "catalogs"),
                Objects.requireNonNull(coordination, "coordination"),
                new CtasTask(tableDdl, dmlDispatch),
                Objects.requireNonNull(metadataDataSource, "metadataDataSource"),
                accessControl);
        KernelDynamicTableService dynamicTables = new KernelDynamicTableService(kernels);
        factory.registerTask(CreateDynamicTableStatement.class, dynamicTables::create);
        factory.registerTask(AlterDynamicTableStatement.class, dynamicTables::alter);
        factory.registerTask(DropDynamicTableStatement.class, dynamicTables::drop);
        factory.registerTask(ShowDynamicTablesStatement.class, dynamicTables::show);
        factory.registerTask(DescribeDynamicTableStatement.class, dynamicTables::describe);
        KernelMaterializedViewService materializedViews =
                new KernelMaterializedViewService(kernels);
        factory.registerTask(CreateMaterializedViewStatement.class, materializedViews::create);
        factory.registerTask(AlterMaterializedViewStatement.class, materializedViews::alter);
        factory.registerTask(RefreshMaterializedViewStatement.class, materializedViews::refresh);
        factory.registerTask(DropMaterializedViewStatement.class, materializedViews::drop);
        factory.registerTask(ShowMaterializedViewsStatement.class, materializedViews::show);
        factory.registerTask(DescribeMaterializedViewStatement.class, materializedViews::describe);
        KernelMaterializationObservabilityService observability =
                new KernelMaterializationObservabilityService(repository);
        factory.registerTask(ShowMaterializationsStatement.class, observability::listObjects);
        factory.registerTask(ShowMaterializationRefreshHistoryStatement.class,
                observability::listRefreshHistory);
        factory.registerTask(ShowMaterializationDependenciesStatement.class,
                observability::listDependencies);
        factory.registerTask(ShowMaterializationGcHistoryStatement.class,
                observability::listGcHistory);
        factory.registerTask(CreateContinuousQueryStatement.class, (statement, context) -> {
            throw new UnsupportedOperationException(
                    "Continuous Query materialization is not implemented");
        });
        return kernels;
    }
}
