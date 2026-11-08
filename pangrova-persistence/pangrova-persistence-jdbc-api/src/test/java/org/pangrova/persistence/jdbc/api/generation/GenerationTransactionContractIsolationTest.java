package org.pangrova.persistence.jdbc.api.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.job.GenerationDispatchSpec;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Loads borrowed-connection contracts on an explicitly minimal, isolated classpath. */
class GenerationTransactionContractIsolationTest {
    @Test
    void jdbcAuthorityPortLinksWithoutDomainImplementationsOrDatabaseDrivers() throws Exception {
        URL[] locations = {
                org.pangrova.security.api.identity.UserIdentity.class.getProtectionDomain().getCodeSource().getLocation(),
                GenerationTransactionAuthority.class.getProtectionDomain().getCodeSource().getLocation(),
                GenerationDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.query.api.ownership.ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("GenerationTransactionAuthority", "GenerationPublicationCandidates",
                    "GenerationJobSubmissionTransactions", "GenerationExecutionTransactionFence",
                    "GenerationTransactionInputs")) {
                var port = loader.loadClass("org.pangrova.persistence.jdbc.api.generation." + name);
                assertThat(port.isInterface()).isTrue();
                assertThat(port.getClassLoader()).isSameAs(loader);
                assertThat(port.getMethods()).isNotEmpty();
            }
            for (String missing : List.of("org.postgresql.Driver", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore",
                    "org.pangrova.generation.codec.execution.GenerationStageGraphCodec",
                    "org.pangrova.security.core.authorization.deferred.DeferredAuthorization",
                    "org.pangrova.metadata.generation.MetadataGenerationTransactionAuthority")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
