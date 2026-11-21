package org.pangrova.worker.function;

import org.pangrova.protocol.execution.AiTaskLifecycle;

import java.util.Optional;
import java.util.ServiceLoader;

/** Selects the single owner of each optional Worker callback. Installed callbacks have no closeable resources. */
public final class WorkerCallbackProviders {
    private WorkerCallbackProviders() {}

    public static AiTaskLifecycle taskLifecycle(ClassLoader loader) {
        return optional(ServiceLoader.load(AiTaskLifecycle.class, loader)).orElse(AiTaskLifecycle.NONE);
    }

    /** Rejects competing owners before any candidate of this service is constructed. */
    public static <T> Optional<T> optional(ServiceLoader<T> installed) {
        var providers = installed.stream().iterator();
        if (!providers.hasNext()) return Optional.empty();
        var first = providers.next();
        if (providers.hasNext()) {
            throw new IllegalStateException("Multiple Worker callback providers: "
                    + first.type().getName() + " and " + providers.next().type().getName());
        }
        return Optional.of(first.get());
    }
}
