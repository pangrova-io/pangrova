package org.pangrova.function.api.runtime;

import org.pangrova.common.api.annotation.Experimental;

import java.util.function.Supplier;

/**
 * Binds the capability authority carried by a Native function callback while a
 * function executes.
 *
 * <p>The Worker function bridge owns callback dispatch but must not depend on
 * any capability runtime. A capability provider installs this scope through
 * {@link java.util.ServiceLoader} so that calls made during the callback
 * observe the exact task identity, deadline and resource grant decoded from
 * the canonical callback payload. When no provider is installed, the bridge
 * evaluates ordinary functions without binding any capability context.</p>
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public interface FunctionCallbackScope {

    /**
     * Runs one callback under the authority encoded in the canonical payload.
     *
     * @param canonicalExecutionContext the canonical callback authority bytes;
     *                                  may be empty for functions without capability context
     * @param callback the evaluation to run
     * @param <T> the callback result type
     * @return the callback result
     * @throws RuntimeException if the authority is invalid or the callback fails
     */
    <T> T call(byte[] canonicalExecutionContext, Supplier<T> callback);
}
