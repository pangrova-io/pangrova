package org.pangrova.worker.function;

import org.pangrova.function.api.runtime.FunctionCallbackScope;

import java.util.function.Supplier;

/** Bridge default for ordinary functions when no capability scope is installed. */
final class UnscopedFunctionCallback implements FunctionCallbackScope {
    @Override
    public <T> T call(byte[] canonicalExecutionContext, Supplier<T> callback) {
        return callback.get();
    }
}
