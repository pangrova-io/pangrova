package org.pangrova.coordinator.materialization;

import java.util.List;
import org.pangrova.materialization.extension.MaterializationExtensionRegistry;
import org.pangrova.materialization.strategy.AppendOnlyRefreshStrategy;
import org.pangrova.materialization.strategy.FullRefreshStrategy;

/** Explicit extension registration for the production materialization kernel. */
public final class MaterializationExtensions {

    private MaterializationExtensions() {
    }

    /**
     * Returns the registered refresh strategies and rewrite extensions.
     *
     * @return production extension registry
     */
    public static MaterializationExtensionRegistry standard() {
        return new MaterializationExtensionRegistry(
                List.of(),
                List.of(new FullRefreshStrategy(), new AppendOnlyRefreshStrategy()),
                List.of(),
                List.of());
    }
}
