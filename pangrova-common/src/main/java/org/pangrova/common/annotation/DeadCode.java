package org.pangrova.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated type has 0% code coverage in the production path
 * and is considered dead code pending removal or activation.
 *
 * <p>Classes annotated with {@code @DeadCode} are excluded from ArchUnit wiring
 * verification checks. Each annotated class must reference a tracking issue that
 * describes the plan: either wire the class into a production entry point or
 * delete it.</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DeadCode {

    /**
     * Tracking issue URL or identifier describing the plan for this dead code
     * (wire into production path or delete).
     */
    String issueReference();
}
