package org.pangrova.generation.core.publication;

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
                "org.pangrova.security.core.authorization.deferred.DeferredAuthorization",
                "org.pangrova.metadata.adapter.generation.MetadataGenerationTransactionAuthority",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap", "org.postgresql.Driver",
                "org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine",
                "org.pangrova.query.core.stage.binding.RetrievalGenerationIntent",
                "org.pangrova.planner.core.analysis.SqlCompiler",
                "org.pangrova.worker.nativebridge.jni.NativeFunctionBridge")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
