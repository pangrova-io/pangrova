package org.pangrova.common.concurrency;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated class is thread-safe.
 *
 * <p>All public methods can be called concurrently from multiple threads
 * without external synchronization. The class documents its concurrency
 * strategy (e.g., ConcurrentHashMap, AtomicReference, volatile) in its
 * Javadoc or field-level comments.</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ThreadSafe {
}
