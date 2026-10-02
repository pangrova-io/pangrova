package org.pangrova.coordinator.testinfra;

import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.TestObservability;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A component-test authority must never silently authorize table reads or replace real policies. */
class SourceFreeQueryFixtureTest {
    @Test
    void sourceFreeProtectedCompilationHasAnExplicitIdentityAndAllPolicies() {
        var context = SourceFreeQueryFixture.withPolicies(unprotected());
        var session = SourceFreeQueryFixture.session("fixture-query");
        var compiler = new SecurityPolicyApplier(context.compiler(), context.accessControlEnforcer(),
                context.columnMaskApplier(), context.rowFilterApplier());
        assertThat(compiler.compileAndApplyPolicies("SELECT 1", session).columnNames()).hasSize(1);
        assertThat(session.userIdentity().principal()).isEqualTo("fixture-subject");
        assertThat(session.tenantId()).isEqualTo("fixture-tenant");
    }

    @Test
    void preservesTheOriginalSchedulingAndCompilationDependencies() {
        var original = unprotected();
        var secured = SourceFreeQueryFixture.withPolicies(original);
        assertThat(secured.compiler()).isSameAs(original.compiler());
        assertThat(secured.scheduling()).isSameAs(original.scheduling());
        assertThat(secured.taskTimeoutSeconds()).isEqualTo(37);
    }

    @Test
    void cannotAuthorizeAnyTableOrAnonymousQuery() {
        var policy = SourceFreeQueryFixture.withPolicies(unprotected()).accessControlEnforcer();
        assertThatThrownBy(() -> policy.checkTableAccess(List.of(List.of("lake", "public", "facts")),
                List.of(), SourceFreeQueryFixture.session("fixture-query").userIdentity()))
                .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class);
        assertThatThrownBy(() -> policy.checkTableAccess(List.of(), List.of(), null))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class);
    }

    @Test
    void refusesToOverwriteAnExistingAuthority() {
        var secured = SourceFreeQueryFixture.withPolicies(unprotected());
        assertThatThrownBy(() -> SourceFreeQueryFixture.withPolicies(secured))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not replace");
    }

    private static QueryDispatcherContext unprotected() {
        return QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new CoordinatorTaskTracker(), null, null, null, 37);
    }
}
