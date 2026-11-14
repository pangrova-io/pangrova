package org.pangrova.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated type is experimental and not yet production-ready.
 *
 * <p>Experimental classes have partial implementations that compile and may pass
 * unit tests, but lack production-grade backends, integration testing, or
 * operational hardening. They may be removed or significantly changed in future
 * releases without notice.</p>
 *
 * <p>Each experimental class must reference a tracking issue that describes the
 * remaining work needed to promote it to stable status.</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExperimentalImplementation {

    /**
     * Tracking issue URL or identifier describing the remaining work
     * needed to promote this class to stable status.
     */
    String trackingIssue();
}
