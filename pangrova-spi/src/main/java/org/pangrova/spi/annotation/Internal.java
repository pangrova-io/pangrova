package org.pangrova.spi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated SPI interface or class is internal to Pangrova.
 *
 * <p>Internal interfaces are not part of the public API and may change or be
 * removed at any time without notice. External connectors and integrations
 * must not depend on internal interfaces.</p>
 *
 * <p>Internal interfaces are used for cross-module communication within
 * Pangrova where the contract is owned by the Pangrova team and not exposed
 * to third-party developers.</p>
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Internal {
}
