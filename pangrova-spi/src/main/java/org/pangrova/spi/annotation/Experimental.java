package org.pangrova.spi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated SPI interface, class, or method is experimental.
 *
 * <p>Experimental APIs are evolving and may have breaking changes (method
 * signature changes, semantic changes, removal) in any minor release. They
 * are exposed to gather feedback and validate the design before being
 * promoted to {@link Stable @Stable}.</p>
 *
 * <p>Connectors and external integrations may depend on experimental APIs
 * but should expect to update their code on minor version upgrades.</p>
 *
 * <p>Once an experimental API stabilises it should be re-annotated with
 * {@link Stable @Stable} and the {@code since} version recorded.</p>
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Experimental {

    /**
     * Optional rationale or tracking issue for the experimental status.
     * Empty string when no specific reason is recorded.
     */
    String reason() default "";
}
