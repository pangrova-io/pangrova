package org.pangrova.security.api.authorization;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Links and constructs the exported snapshot with all Security implementations unavailable. */
class DeferredAuthorityContractIsolationTest {
    @Test
    void snapshotLoadsAndConstructsWithoutJsonOrSecurityImplementations() throws Exception {
        URL[] locations = {
                DeferredAuthorizationScope.class.getProtectionDomain().getCodeSource().getLocation(),
                QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                PangrovaException.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.security.")
                        && !name.startsWith("org.pangrova.security.api.")) {
                    throw new ClassNotFoundException("Security implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (String missing : List.of("com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec",
                    "org.pangrova.security.core.authorization.deferred.DeferredAuthorization")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            Class<?> scopeType = loader.loadClass(DeferredAuthorizationScope.class.getName());
            Class<?> tableType = loader.loadClass(QualifiedTableName.class.getName());
            Object table = tableType.getConstructor(String.class, String.class, String.class)
                    .newInstance("lake", "sales", "facts");
            Object scope = scopeType.getMethod("raw", tableType, Set.class, tableType)
                    .invoke(null, table, Set.of("amount"), table);
            assertThat(scopeType.getMethod("permissions").invoke(scope)).asList().hasSize(2);
            for (Class<?> nested : scopeType.getDeclaredClasses()) {
                assertThat(nested.getDeclaredMethods()).isNotEmpty();
            }
            assertThat(scopeType.getMethod("equals", Object.class).invoke(scope, scope)).isEqualTo(true);
        }
    }
}
