package org.pangrova.common.concurrency;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated field or method must only be accessed
 * while holding the specified lock.
 *
 * <p>Example: {@code @GuardedBy("this")} or {@code @GuardedBy("lock")}</p>
 */
@Documented
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface GuardedBy {

    /**
     * The lock that guards the annotated field or method.
     *
     * @return lock expression (e.g., "this", "lock", "ClassName.class")
     */
    String value();
}
