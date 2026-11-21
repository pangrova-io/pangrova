package org.pangrova.materialization.strategy;

import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.refresh.RefreshStrategy;
import org.pangrova.materialization.api.refresh.RefreshStrategyFactory;

/** Supplies the AppendOnly algorithm to the Coordinator's materialization registry. */
public final class AppendOnlyRefreshStrategyFactory implements RefreshStrategyFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public RefreshKind kind() {
        return RefreshKind.INCREMENTAL_APPEND_ONLY;
    }

    @Override
    public RefreshStrategy create() {
        return new AppendOnlyRefreshStrategy();
    }
}
