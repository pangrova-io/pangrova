package org.pangrova.security.authn;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SecureJwtParser} and its sensitive entry points are NOT exposed
 * as a public API surface (Task 7.7, Requirement 8.7).
 *
 * <p>Background: {@code parseUnsignedToken} processes unsigned JWTs (alg=none) without
 * signature verification. Even though the calling authenticator only invokes it when
 * signature verification is explicitly disabled, the method itself must never be a
 * reachable public API — otherwise external code (or reflection-friendly callers) could
 * craft an alg=none token and bypass authentication entirely.
 *
 * <p>This test enforces the invariant via reflection so any future widening of the
 * modifier (e.g., refactoring that accidentally adds {@code public}) immediately fails.
 *
 * <p>No mocks: uses {@link Class#forName} and {@link Method#getModifiers()} only.
 */
class SecureJwtParserVisibilityTest {

    private static final String FQCN = "org.pangrova.security.authn.SecureJwtParser";

    @Test
    void classIsNotPublic() throws Exception {
        Class<?> clazz = Class.forName(FQCN);
        int modifiers = clazz.getModifiers();
        assertThat(Modifier.isPublic(modifiers))
                .as("SecureJwtParser must be package-private (not public)")
                .isFalse();
    }

    @Test
    void classIsFinal() throws Exception {
        Class<?> clazz = Class.forName(FQCN);
        int modifiers = clazz.getModifiers();
        assertThat(Modifier.isFinal(modifiers))
                .as("SecureJwtParser must be final to prevent subclassing that widens visibility")
                .isTrue();
    }

    @Test
    void parseSignedTokenIsNotPublic() throws Exception {
        Method method = lookupDeclared("parseSignedToken", String.class);
        int modifiers = method.getModifiers();
        assertThat(Modifier.isPublic(modifiers))
                .as("parseSignedToken must NOT be public — only the authenticator should call it")
                .isFalse();
        assertThat(Modifier.isProtected(modifiers))
                .as("parseSignedToken must NOT be protected")
                .isFalse();
    }

    @Test
    void noPublicMethodReturnsParsedJwt() throws Exception {
        Class<?> clazz = Class.forName(FQCN);
        for (Method m : clazz.getDeclaredMethods()) {
            if (!Modifier.isPublic(m.getModifiers())) {
                continue;
            }
            String returnTypeName = m.getReturnType().getName();
            assertThat(returnTypeName)
                    .as("No public method on SecureJwtParser may expose ParsedJwt: found %s", m.getName())
                    .doesNotContain("ParsedJwt");
        }
    }

    @Test
    void constructorIsNotPublic() throws Exception {
        Class<?> clazz = Class.forName(FQCN);
        for (var ctor : clazz.getDeclaredConstructors()) {
            assertThat(Modifier.isPublic(ctor.getModifiers()))
                    .as("SecureJwtParser constructors must NOT be public")
                    .isFalse();
        }
    }

    private static Method lookupDeclared(String name, Class<?>... params) throws Exception {
        Class<?> clazz = Class.forName(FQCN);
        return clazz.getDeclaredMethod(name, params);
    }
}
