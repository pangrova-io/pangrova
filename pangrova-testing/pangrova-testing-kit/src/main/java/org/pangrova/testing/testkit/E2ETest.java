package org.pangrova.testing.testkit;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Meta-annotation marking a test class or method as an E2E (end-to-end) scenario test.
 *
 * <p>E2E tests validate complete user-facing scenarios against a running cluster
 * (either in-JVM or Docker-container-based). They are excluded from the default
 * Maven build and only run with the {@code -Pe2e} profile:
 * <pre>{@code
 *   mvn verify -Pe2e
 * }</pre>
 *
 * <p>Combines:
 * <ul>
 *   <li>{@code @Tag("e2e")} — Maven Surefire/Failsafe group filter</li>
 *   <li>{@code @Tag("integration")} — inherited integration flag</li>
 * </ul>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Tag("e2e")
@Tag("integration")
public @interface E2ETest {
}
