package org.pangrova.streaming.core.cdc;

import java.util.Optional;

/**
 * Thread-local holder for the active pipeline's security context.
 *
 * <p>The CDC pipeline loop installs the {@link PipelineSecurityContext} at the
 * start of its dedicated thread. Downstream components (write sinks, connector
 * sessions) can retrieve the active context via {@link #get()} to enforce
 * RLS, column masking, and authorization policies based on the pipeline
 * creator's identity.</p>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>Each pipeline runs on a dedicated thread. The context is set once at
 * thread start and cleared on thread exit. There is no cross-thread sharing
 * of the holder — each thread sees only its own context.</p>
 *
 * <h3>Usage Pattern</h3>
 * <pre>{@code
 * // In CdcPipelineLoop.run():
 * PipelineSecurityContextHolder.set(context);
 * try {
 *     runPipelineLoop();
 * } finally {
 *     PipelineSecurityContextHolder.clear();
 * }
 *
 * // In CdcWriteSink implementation:
 * PipelineSecurityContext ctx = PipelineSecurityContextHolder.get()
 *     .orElseThrow(() -> new SecurityException("No security context"));
 * String user = ctx.principal();
 * }</pre>
 *
 * @see PipelineSecurityContext
 * @see CdcPipelineLoop
 */
public final class PipelineSecurityContextHolder {

    private static final ThreadLocal<PipelineSecurityContext> CONTEXT = new ThreadLocal<>();

    private PipelineSecurityContextHolder() {
        // utility class
    }

    /**
     * Sets the security context for the current thread.
     *
     * @param context the pipeline security context, must not be null
     * @throws NullPointerException if context is null
     */
    public static void set(PipelineSecurityContext context) {
        if (context == null) {
            throw new NullPointerException("PipelineSecurityContext must not be null");
        }
        CONTEXT.set(context);
    }

    /**
     * Returns the security context for the current thread, if set.
     *
     * @return the active security context, or empty if not in a pipeline thread
     */
    public static Optional<PipelineSecurityContext> get() {
        return Optional.ofNullable(CONTEXT.get());
    }

    /**
     * Returns the security context for the current thread, throwing if absent.
     *
     * <p>Use this in code paths that must always have a security context
     * (e.g., write sinks that enforce RLS). The exception indicates a
     * programming error — a pipeline was started without a security context.</p>
     *
     * @return the active security context
     * @throws IllegalStateException if no security context is set
     */
    public static PipelineSecurityContext require() {
        PipelineSecurityContext ctx = CONTEXT.get();
        if (ctx == null) {
            throw new IllegalStateException(
                    "No PipelineSecurityContext set on current thread. "
                            + "CDC pipelines must always carry a security context.");
        }
        return ctx;
    }

    /**
     * Clears the security context for the current thread.
     * Must be called in a finally block when the pipeline thread exits.
     */
    public static void clear() {
        CONTEXT.remove();
    }
}
