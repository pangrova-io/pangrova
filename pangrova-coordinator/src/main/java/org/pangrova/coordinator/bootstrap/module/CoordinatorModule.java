package org.pangrova.coordinator.bootstrap.module;

/**
 * A self-contained unit of coordinator functionality that can be independently
 * configured and loaded during bootstrap. Modules declare their bindings,
 * query phases, stage handlers, and completion listeners via the provided context.
 *
 * <p>Modules are loaded in {@link #priority()} ascending order. Lower priority
 * values are loaded first, allowing core modules to bind services that optional
 * modules depend on.</p>
 */
public interface CoordinatorModule {

    /**
     * Configures this module by registering bindings and components
     * with the provided context.
     *
     * @param context the module context for registering bindings and components
     */
    void configure(CoordinatorModuleContext context);

    /**
     * Returns the loading priority. Lower values are loaded first.
     * Core modules should use 0-100, optional modules 100-1000.
     *
     * @return the priority value (default 100)
     */
    default int priority() {
        return 100;
    }
}
