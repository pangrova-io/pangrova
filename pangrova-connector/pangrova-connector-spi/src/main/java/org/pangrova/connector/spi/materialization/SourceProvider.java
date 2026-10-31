package org.pangrova.connector.spi.materialization;

import org.pangrova.common.api.annotation.Stable;

/**
 * Connector-side source contract for materialization.
 *
 * <p>Implementations live in connector modules and depend only on this SPI.
 * Every method is explicit about failure: a connector that cannot pin a
 * snapshot, enumerate changes, or prove a change interval must throw
 * {@link MaterializationSourceException} rather than return an empty or
 * optimistic result. The kernel converts those failures into
 * {@code INCREMENTAL_UNSUPPORTED} or a failed attempt; there is no implicit
 * fallback to a weaker execution mode.
 *
 * <p>Implementations must be thread-safe.
 */
@Stable(since = "1.0")
public interface SourceProvider {

    /**
     * Stable connector identifier, matching {@link SourceRef#connectorId()}.
     *
     * @return connector identifier
     */
    String connectorId();

    /**
     * Returns the versioned capability descriptor for a source table.
     *
     * @param source source table
     * @return capability descriptor
     */
    MaterializationCapabilities capabilities(SourceRef source);

    /**
     * Returns the source columns a partition-replace change set identifies
     * partitions by.
     *
     * <p>Planning uses this to verify that a definition exposes every
     * partition column before offering partition replacement. A source
     * without identity partitioning, or a connector that cannot prove
     * partition replacement, returns an empty list.
     *
     * @param source source table
     * @return partition column names, empty when partition replacement is unavailable
     */
    default java.util.List<String> partitionColumns(SourceRef source) {
        return java.util.List.of();
    }

    /**
     * Describes an exact source snapshot previously observed by the planner.
     *
     * <p>The provider is the authority for snapshot identity, schema
     * fingerprint, and pinnability. A token that does not designate an
     * existing immutable snapshot must fail with
     * {@link MaterializationSourceException}; a provider must never substitute
     * the current snapshot for the requested token.
     *
     * @param source source table
     * @param tenant tenant on whose behalf the source is read
     * @param snapshotToken connector-specific immutable snapshot identifier
     * @param policyFingerprint fingerprint of the policies the caller will enforce
     * @return authoritative snapshot identity
     */
    SourceSnapshotRef describe(
            SourceRef source, String tenant, String snapshotToken, String policyFingerprint);

    /**
     * Describes a source that has no data snapshot yet.
     *
     * <p>An empty source can be materialized in full but has no interval to
     * track incrementally. The provider is the authority for the schema
     * fingerprint and for the immutable empty-state identity supplied by the
     * planner; the returned token must use
     * {@link SourceSnapshotRef#EMPTY_TOKEN_PREFIX} so the kernel can recognize
     * that no snapshot selector exists. A provider that cannot retain or
     * verify the empty state must fail with {@link MaterializationSourceException}.
     *
     * @param source source table
     * @param tenant tenant on whose behalf the source is read
     * @param sourceIdentity connector-owned identity of the observed empty state
     * @param policyFingerprint fingerprint of the policies the caller will enforce
     * @return authoritative empty-snapshot identity
     */
    default SourceSnapshotRef describeEmpty(
            SourceRef source, String tenant, String sourceIdentity, String policyFingerprint) {
        throw new MaterializationSourceException(
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                "Connector " + connectorId()
                        + " cannot materialize a source without a data snapshot");
    }

    /**
     * Pins an exact snapshot for the duration of an attempt.
     *
     * <p>The pin owner scopes the physical hold. Pinning the same snapshot
     * twice under different owners must create two independent holds; pinning
     * twice under the same owner is idempotent. If the requested snapshot is
     * no longer available, the provider must fail with
     * {@link MaterializationSourceException} instead of pinning a newer one.
     *
     * @param source source table
     * @param tenant tenant on whose behalf the source is read
     * @param expected exact snapshot observed while planning
     * @param pinOwner durable identity that owns the pin, normally the attempt id
     * @return source with its pinned snapshot
     */
    PinnedSource pin(
            SourceRef source, String tenant, SourceSnapshotRef expected, String pinOwner);

    /**
     * Plans the change set between two pinned snapshots.
     *
     * @param from base snapshot, inclusive
     * @param to target snapshot, inclusive
     * @param mode requested change-set mode
     * @return change-set plan
     */
    ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode);

    /**
     * Verifies that a change-set plan was fully evaluated.
     *
     * @param plan plan to verify
     * @return change proof; callers must check {@link ChangeProof#complete()}
     */
    ChangeProof verify(ChangeSetPlan plan);

    /**
     * Releases a previously pinned snapshot. Calling this for a snapshot that
     * is not held by {@link PinnedSource#pinOwner()} must be a no-op.
     *
     * @param pinned source snapshot hold to release
     */
    void unpin(PinnedSource pinned);
}
