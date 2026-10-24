package org.pangrova.materialization.domain;

import java.util.Map;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Executable plan produced by a refresh strategy.
 *
 * @param kind resolved refresh kind
 * @param canonicalSql canonical SQL submitted to the data plane
 * @param changeSets per-source change-set plans; empty for full refresh
 * @param planFingerprint canonical hash of the strategy plan
 */
public record RefreshPlan(
        RefreshKind kind,
        String canonicalSql,
        Map<SourceRef, ChangeSetPlan> changeSets,
        String planFingerprint) {

    public RefreshPlan {
        if (kind == null) {
            throw new IllegalArgumentException("kind must not be null");
        }
        if (canonicalSql == null || canonicalSql.isBlank()) {
            throw new IllegalArgumentException("canonicalSql must not be blank");
        }
        changeSets = changeSets == null ? Map.of() : Map.copyOf(changeSets);
        if (planFingerprint == null || planFingerprint.isBlank()) {
            throw new IllegalArgumentException("planFingerprint must not be blank");
        }
        if (!kind.isIncremental() && !changeSets.isEmpty()) {
            throw new IllegalArgumentException("a full refresh plan must not carry change sets");
        }
        if (kind.isIncremental() && changeSets.isEmpty()) {
            throw new IllegalArgumentException("an incremental plan must carry change sets");
        }
    }
}
