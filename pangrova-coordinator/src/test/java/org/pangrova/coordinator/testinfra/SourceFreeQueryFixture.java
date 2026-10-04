package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.SecurityContextBundle;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.InMemoryRoleStore;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Explicit identity and deny-all table authority for source-free control-plane component tests. */
public final class SourceFreeQueryFixture {
    private SourceFreeQueryFixture() {}

    /** Fills missing policy dependencies without granting access to any catalog or table. */
    public static QueryDispatcherContext withPolicies(QueryDispatcherContext context) {
        if (context.accessControlEnforcer() != null || context.columnMaskApplier() != null
                || context.rowFilterApplier() != null) {
            throw new IllegalArgumentException("Fixture must not replace an existing policy authority");
        }
        return new QueryDispatcherContext(context.compiler(), policies(), context.scheduling(),
                context.catalogRegistry(), context.lineageEmitter(), context.taskTimeoutSeconds(),
                context.runtimeFilterRegistry(), context.adaptiveQueryExecutor(), context.adaptiveQueryConfig(),
                context.shuffleLifecycleManager(), context.haService(), context.coordinatorId(),
                context.stateJournal(), context.indexAwareSplitFetcher(), context.embeddingModelRegistry());
    }

    /** Creates complete production policy processors with no table grants for compiler component tests. */
    public static SecurityContextBundle policies() {
        var access = new DynamicRbacAccessControl(new StaticGrantStore(List.of()), new InMemoryRoleStore(),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
        return new SecurityContextBundle(new AccessControlEnforcer(access),
                new ColumnMaskApplier(access), new RowFilterApplier(access));
    }

    /** Creates only a component-test identity; it is not HTTP authentication evidence. */
    public static CoordinatorSessionContext session(String queryId) {
        return new CoordinatorSessionContext(queryId, Map.of(),
                new UserIdentity("fixture-subject", Set.of(), Map.of(), Instant.EPOCH), "fixture-tenant");
    }
}
