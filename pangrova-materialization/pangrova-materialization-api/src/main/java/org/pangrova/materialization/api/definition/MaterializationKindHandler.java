package org.pangrova.materialization.api.definition;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.enums.RefreshMode;

/**
 * Extension that owns kind-specific lifecycle semantics.
 *
 * <p>Dynamic Table and Materialized View share the lifecycle, scheduling,
 * publication, recovery, and garbage-collection kernel. Only the differences
 * that are genuinely kind-specific are expressed here, so a future object kind
 * can be added without changing the kernel.
 */
@Experimental
public interface MaterializationKindHandler {

    /**
     * Returns the kind served by this handler.
     *
     * @return object kind
     */
    MaterializationKind kind();

    /**
     * Returns the refresh mode used when the DDL omits one.
     *
     * @return default refresh mode
     */
    RefreshMode defaultRefreshMode();

    /**
     * Validates kind-specific definition constraints.
     *
     * @param definition definition revision
     */
    void validateDefinition(DefinitionRevision definition);

    /**
     * Returns whether objects of this kind participate in transparent rewrite.
     *
     * @return true for kinds that may replace equivalent query plans
     */
    boolean supportsTransparentRewrite();
}
