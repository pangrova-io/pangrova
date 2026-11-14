package org.pangrova.materialization.strategy;

import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.refresh.RefreshStrategy;
import org.pangrova.materialization.api.refresh.RefreshStrategyFactory;

/** Supplies the Full algorithm to the Coordinator's materialization registry. */
public final class FullRefreshStrategyFactory implements RefreshStrategyFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public RefreshKind kind() {
        return RefreshKind.FULL;
    }

    @Override
    public RefreshStrategy create() {
        return new FullRefreshStrategy();
    }
}
