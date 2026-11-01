package org.pangrova.materialization.store.jdbc;

import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.RefreshKind;

/**
 * State-code translation between persisted materialization columns and the
 * domain state singletons.
 *
 * <p>Decoding fails closed on unknown persisted values so a corrupted row can
 * never become a domain object.</p>
 */
final class JdbcMaterializationStateCodec {

    private JdbcMaterializationStateCodec() {
    }

    static ObjectState objectState(String value) {
        return switch (value) {
            case "CREATING" -> ObjectState.Creating.INSTANCE;
            case "ACTIVE" -> ObjectState.Active.INSTANCE;
            case "SUSPENDED" -> ObjectState.Suspended.INSTANCE;
            case "FAILED" -> ObjectState.Failed.INSTANCE;
            case "DROPPING" -> ObjectState.Dropping.INSTANCE;
            case "DROPPED" -> ObjectState.Dropped.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown object state " + value);
        };
    }

    static AttemptState attemptState(String value) {
        return switch (value) {
            case "PREPARING" -> AttemptState.Preparing.INSTANCE;
            case "BUILDING" -> AttemptState.Building.INSTANCE;
            case "COMMITTED" -> AttemptState.Committed.INSTANCE;
            case "PUBLISHED" -> AttemptState.Published.INSTANCE;
            case "FAILED" -> AttemptState.Failed.INSTANCE;
            case "ABANDONED" -> AttemptState.Abandoned.INSTANCE;
            case "QUARANTINED" -> AttemptState.Quarantined.INSTANCE;
            case "SUPERSEDED" -> AttemptState.Superseded.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown attempt state " + value);
        };
    }

    static GenerationState generationState(String value) {
        return switch (value) {
            case "CANDIDATE" -> GenerationState.Candidate.INSTANCE;
            case "COMMITTED" -> GenerationState.Committed.INSTANCE;
            case "PUBLISHED" -> GenerationState.Published.INSTANCE;
            case "RETIRED" -> GenerationState.Retired.INSTANCE;
            case "QUARANTINED" -> GenerationState.Quarantined.INSTANCE;
            case "COLLECTED" -> GenerationState.Collected.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown generation state " + value);
        };
    }

    static RefreshKind refreshKind(String value) {
        return switch (value) {
            case "FULL" -> RefreshKind.Full.INSTANCE;
            case "INCREMENTAL_APPEND_ONLY" -> RefreshKind.IncrementalAppendOnly.INSTANCE;
            case "INCREMENTAL_PARTITION_REPLACE" -> RefreshKind.IncrementalPartitionReplace.INSTANCE;
            case "INCREMENTAL_AFFECTED_KEY" -> RefreshKind.IncrementalAffectedKey.INSTANCE;
            case "INCREMENTAL_CDC" -> RefreshKind.IncrementalCdc.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown refresh kind " + value);
        };
    }

    static String refreshKindName(RefreshKind kind) {
        if (kind == RefreshKind.Full.INSTANCE) {
            return "FULL";
        }
        if (kind == RefreshKind.IncrementalAppendOnly.INSTANCE) {
            return "INCREMENTAL_APPEND_ONLY";
        }
        if (kind == RefreshKind.IncrementalPartitionReplace.INSTANCE) {
            return "INCREMENTAL_PARTITION_REPLACE";
        }
        if (kind == RefreshKind.IncrementalAffectedKey.INSTANCE) {
            return "INCREMENTAL_AFFECTED_KEY";
        }
        return "INCREMENTAL_CDC";
    }

    static String objectStateName(ObjectState state) {
        if (state == ObjectState.Creating.INSTANCE) {
            return "CREATING";
        }
        if (state == ObjectState.Active.INSTANCE) {
            return "ACTIVE";
        }
        if (state == ObjectState.Suspended.INSTANCE) {
            return "SUSPENDED";
        }
        if (state == ObjectState.Failed.INSTANCE) {
            return "FAILED";
        }
        if (state == ObjectState.Dropping.INSTANCE) {
            return "DROPPING";
        }
        return "DROPPED";
    }

    static String attemptStateName(AttemptState state) {
        if (state == AttemptState.Preparing.INSTANCE) {
            return "PREPARING";
        }
        if (state == AttemptState.Building.INSTANCE) {
            return "BUILDING";
        }
        if (state == AttemptState.Committed.INSTANCE) {
            return "COMMITTED";
        }
        if (state == AttemptState.Published.INSTANCE) {
            return "PUBLISHED";
        }
        if (state == AttemptState.Failed.INSTANCE) {
            return "FAILED";
        }
        if (state == AttemptState.Abandoned.INSTANCE) {
            return "ABANDONED";
        }
        if (state == AttemptState.Quarantined.INSTANCE) {
            return "QUARANTINED";
        }
        return "SUPERSEDED";
    }

    static String generationStateName(GenerationState state) {
        if (state == GenerationState.Candidate.INSTANCE) {
            return "CANDIDATE";
        }
        if (state == GenerationState.Committed.INSTANCE) {
            return "COMMITTED";
        }
        if (state == GenerationState.Published.INSTANCE) {
            return "PUBLISHED";
        }
        if (state == GenerationState.Retired.INSTANCE) {
            return "RETIRED";
        }
        if (state == GenerationState.Quarantined.INSTANCE) {
            return "QUARANTINED";
        }
        return "COLLECTED";
    }
}
