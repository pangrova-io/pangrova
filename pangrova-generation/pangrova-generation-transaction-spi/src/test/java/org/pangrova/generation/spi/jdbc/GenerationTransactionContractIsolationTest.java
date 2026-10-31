package org.pangrova.generation.spi.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

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
                GenerationTransactionAuthority.class.getProtectionDomain().getCodeSource().getLocation(),
                GenerationDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                DeferredAuthorizationScope.class.getProtectionDomain().getCodeSource().getLocation(),
                ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.metadata.")
                        || name.startsWith("org.pangrova.generation.store.")
                        || (name.startsWith("org.pangrova.security.")
                        && !name.startsWith("org.pangrova.security.api."))) {
                    throw new ClassNotFoundException("Domain implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (String name : List.of("GenerationTransactionAuthority", "GenerationPublicationCandidates",
                    "GenerationJobSubmissionTransactions", "GenerationExecutionTransactionFence",
                    "GenerationTransactionInputs")) {
                var port = loader.loadClass("org.pangrova.generation.spi.jdbc." + name);
                assertThat(port.isInterface()).isTrue();
                assertThat(port.getClassLoader()).isSameAs(loader);
                assertThat(port.getMethods()).isNotEmpty();
            }
            for (String missing : List.of("org.postgresql.Driver", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.metadata.adapter.generation.MetadataGenerationTransactionAuthority")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
