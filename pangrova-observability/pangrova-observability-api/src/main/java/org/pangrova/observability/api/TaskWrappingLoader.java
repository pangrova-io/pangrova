package org.pangrova.observability.api;

import java.util.ServiceLoader;

/**
 * Lazy holder for the {@link Context.TaskWrapping} singleton, loaded via ServiceLoader.
 *
 * <p>Thread safety: relies on JVM class loading guarantees for safe lazy initialization.
 */
final class TaskWrappingLoader {

    static final Context.TaskWrapping INSTANCE = load();

    private TaskWrappingLoader() {}

    private static Context.TaskWrapping load() {
        ServiceLoader<Context.TaskWrapping> loader = ServiceLoader.load(Context.TaskWrapping.class);
        return loader.findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No Context.TaskWrapping implementation found on classpath. "
                                + "Ensure pangrova-observability is on the module path."));
    }
}
