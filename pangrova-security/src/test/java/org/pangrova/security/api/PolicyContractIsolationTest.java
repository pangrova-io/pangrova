package org.pangrova.security.api;

import org.pangrova.spi.security.AccessDeniedException;

import org.pangrova.common.exception.PangrovaException;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises policy constructors with only the Security API and Common available. */
class PolicyContractIsolationTest {

    @Test
    void policyConstructionAndRejectionDoNotLoadImplementations() throws Exception {
        URL[] locations = {
                AccessDeniedException.class.getProtectionDomain().getCodeSource().getLocation(),
                PangrovaException.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (URLClassLoader loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.")
                        && !name.startsWith("org.pangrova.spi.security.")
                        && !name.startsWith("org.pangrova.common.")) {
                    throw new ClassNotFoundException("Implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            assertThatThrownBy(() -> loader.loadClass("org.pangrova.security.core.authz.SecurityStatementHandler"))
                    .isInstanceOf(ClassNotFoundException.class);
            for (String policy : List.of("rowfilter.RowPolicy", "mask.ColumnMask")) {
                Class<?> type = loader.loadClass("org.pangrova.spi.security." + policy);
                Constructor<?> constructor = type.getDeclaredConstructor(Arrays.stream(type.getRecordComponents())
                        .map(component -> component.getType()).toArray(Class<?>[]::new));
                Object[] valid = arguments(type, "drop_count > 0");
                assertThat(constructor.newInstance(valid)).isInstanceOf(type);
                try {
                    constructor.newInstance(arguments(type, "backdrop; DROP TABLE data"));
                    throw new AssertionError("Policy must reject a standalone DDL keyword");
                } catch (InvocationTargetException error) {
                    Throwable rejection = error.getCause();
                    assertThat(rejection.getClass().getName())
                            .isEqualTo("org.pangrova.spi.security.AuthorizationDeniedException");
                    assertThat(rejection.getClass().getMethod("getErrorCode").invoke(rejection).toString())
                            .isEqualTo("ACCESS_DENIED");
                }
            }
        }
    }

    private static Object[] arguments(Class<?> type, String expression) {
        return Arrays.stream(type.getRecordComponents()).map(component -> {
            if (component.getName().endsWith("Expression")) {
                return expression;
            }
            if (component.getType() == String.class) {
                return "test";
            }
            if (component.getType() == long.class) {
                return 1L;
            }
            if (component.getType() == int.class) {
                return 1;
            }
            if (component.getType() == boolean.class) {
                return true;
            }
            if (component.getType() == List.class) {
                return List.of();
            }
            return null;
        }).toArray();
    }
}
