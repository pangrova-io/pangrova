package org.pangrova.spi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated SPI interface or class has a stable contract.
 *
 * <p>Stable interfaces will not have breaking changes (method removal,
 * signature changes, semantic changes) within the same major version.
 * Additions (new default methods) are permitted.</p>
 *
 * <p>Connectors and external integrations can safely depend on stable APIs
 * without risk of breakage across minor version upgrades.</p>
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Stable {

    /**
     * The version since which this interface has been stable.
     * Format: major.minor (e.g., "1.0").
     */
    String since();
}
