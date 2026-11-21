package org.pangrova.generation.publication;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Codecs and routing must not pull backend or authorization implementations into consumers. */
class GenerationCoreDependencyIsolationTest {
    @Test
    void realCoreClasspathContainsPortsButNoProvidersOrSecurityRuntime() throws Exception {
        var loader = getClass().getClassLoader();
        assertThat(Class.forName(GenerationJobStore.class.getName(), false, loader)).isSameAs(GenerationJobStore.class);
        for (String name : List.of("org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore",
                "org.pangrova.security.authorization.deferred.DeferredAuthorization",
                "org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationTransactionAuthority",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap", "org.postgresql.Driver",
                "org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine",
                "org.pangrova.execution.stage.binding.RetrievalGenerationIntent",
                "org.pangrova.planner.analysis.SqlCompiler",
                "org.pangrova.worker.function.NativeFunctionCallbacks")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
