package org.pangrova.metadata.testinfra;

import java.util.Objects;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.catalog.FederationHandleFactory;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.table.api.provider.TableStores;
import org.pangrova.table.api.policy.TablePolicyRepository;
import org.pangrova.table.api.statistics.TableControlStatisticsStore;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.policy.TableControlPolicyBinder;
import org.pangrova.table.statistics.TableControlStatisticsService;

/** Test-scoped registry for cases exercising different ownership settings in one running fixture. */
public class TestCatalogRegistry extends CatalogRegistry {
    private final TableControlStatisticsStore statisticsStore;
    private volatile TableControlService control;
    private volatile TableControlStatisticsService statistics;
    private volatile TablePolicyRepository bindings;

    public TestCatalogRegistry(TableStores stores, CatalogDeploymentRepository deployments) {
        this(stores, deployments, null, null, null);
    }

    public TestCatalogRegistry(TableStores stores, CatalogDeploymentRepository deployments,
            ConnectorHandleFactory connectors, FederationHandleFactory federations, MetadataEventDispatcher events) {
        super(stores, deployments, connectors, federations, events);
        statisticsStore = stores.statistics();
        control = super.tableControl();
        statistics = super.tableStatistics();
        bindings = stores.governance();
    }

    public void setTableControlService(TableControlService service) {
        control = Objects.requireNonNull(service);
        statistics = new TableControlStatisticsService(service, statisticsStore);
    }

    public void setGovernanceBindingStore(TablePolicyRepository store) {
        bindings = Objects.requireNonNull(store);
    }

    @Override
    public TableControlService tableControl() {
        return control;
    }

    @Override
    public TableControlStatisticsService tableStatistics() {
        return statistics;
    }

    @Override
    public TableControlPolicyBinder tableGovernance() {
        return new TableControlPolicyBinder(control, bindings);
    }
}
