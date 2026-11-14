package org.pangrova.materialization.kernel;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.Binding;
import org.pangrova.materialization.api.domain.ColumnDefinition;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.enums.DependencyKind;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.ExecutionFence;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.IncarnationId;
import org.pangrova.materialization.api.domain.InternalNamespace;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.domain.PolicySnapshot;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.enums.SecurityMode;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.materialization.api.domain.StrategyId;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.enums.TriggerKind;
import org.pangrova.materialization.extension.MaterializationExtensionRegistry;
import org.pangrova.materialization.api.port.SourcePinning;
import org.pangrova.connector.api.materialization.ChangeProof;
import org.pangrova.connector.api.enums.ChangeSetMode;
import org.pangrova.connector.api.materialization.ChangeSetPlan;
import org.pangrova.connector.api.materialization.CommitEvidence;
import org.pangrova.connector.api.exception.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

/** Shared, deterministic fixtures for materialization domain and extension tests. */
public final class MaterializationTestFixtures {

    public static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    public static final Instant LATER = NOW.plus(Duration.ofMinutes(5));
    public static final TenantId TENANT = new TenantId("tenant-a");
    public static final IncarnationId INCARNATION =
            new IncarnationId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    public static final GenerationId GENERATION_ID =
            new GenerationId(UUID.fromString("00000000-0000-0000-0000-0000000000aa"));
    public static final AttemptId ATTEMPT_ID =
            new AttemptId(UUID.fromString("00000000-0000-0000-0000-0000000000bb"));
    public static final Principal DEFINER = new Principal("definer");
    public static final Principal REQUESTER = new Principal("requester");

    private MaterializationTestFixtures() {
    }

    public static MaterializationId objectId(String name) {
        return new MaterializationId(TENANT, QualifiedName.parse("db." + name));
    }

    public static Principal principal(String name) {
        return new Principal(name);
    }

    public static SourceRef source(String table) {
        return new SourceRef("iceberg", "lake", "db", table);
    }

    public static SourceSnapshotRef snapshot(SourceRef source, String token) {
        return snapshot(source, token, true);
    }

    public static SourceSnapshotRef snapshot(SourceRef source, String token, boolean pinnable) {
        return new SourceSnapshotRef(
                source.connectorId(),
                token,
                "schema-v1",
                "policy-v1",
                NOW,
                pinnable);
    }

    public static SourceVector vector(SourceRef... sources) {
        Map<SourceRef, SourceSnapshotRef> snapshots = new LinkedHashMap<>();
        for (int i = 0; i < sources.length; i++) {
            snapshots.put(sources[i], snapshot(sources[i], "snap-" + i));
        }
        return new SourceVector(snapshots, 1);
    }

    public static SourceVector vector(Map<SourceRef, SourceSnapshotRef> snapshots, long version) {
        return new SourceVector(snapshots, version);
    }

    public static OutputSchema outputSchema() {
        return new OutputSchema(List.of(
                new ColumnDefinition("id", "BIGINT", false, 0),
                new ColumnDefinition("name", "VARCHAR", true, 1)));
    }

    public static Binding binding(String sql) {
        return new Binding(sql, "plan-fingerprint");
    }

    public static PolicySnapshot policies(Set<SourceRef> sources) {
        Map<SourceRef, String> fingerprints = new LinkedHashMap<>();
        for (SourceRef source : sources) {
            fingerprints.put(source, "policy-fingerprint");
        }
        return new PolicySnapshot(fingerprints);
    }

    public static MaterializationObject header(MaterializationId object) {
        return MaterializationObject.creating(object, MaterializationKind.DYNAMIC_TABLE,
                INCARNATION, DEFINER, SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(),
                NOW);
    }

    public static DefinitionRevision definition(MaterializationId object, SourceVector vector) {
        return definition(object, 1, "SELECT id, name FROM orders", vector.snapshots().keySet());
    }

    public static DefinitionRevision definition(MaterializationId object, long revision, String sql) {
        Set<SourceRef> sources = Set.of(source("orders"));
        return definition(object, revision, sql, sources, Set.of());
    }

    public static DefinitionRevision definition(
            MaterializationId object, long revision, String sql, Set<SourceRef> sources) {
        return definition(object, revision, sql, sources, Set.of());
    }

    public static DefinitionRevision definition(
            MaterializationId object,
            long revision,
            String sql,
            Set<SourceRef> sources,
            Set<Dependency> dependencies) {
        return new DefinitionRevision(
                object,
                revision,
                sql,
                binding(sql),
                outputSchema(),
                sources,
                dependencies,
                policies(sources),
                SecurityMode.DEFINER,
                DEFINER,
                Map.of(),
                NOW);
    }

    public static MaterializationObject creating(MaterializationKind kind, RefreshMode mode) {
        return MaterializationObject.creating(
                objectId("orders_dt"),
                kind,
                INCARNATION,
                DEFINER,
                SecurityMode.DEFINER,
                mode,
                mode == RefreshMode.FULL ? Optional.empty() : Optional.of(StrategyId.APPEND_ONLY),
                NOW);
    }

    public static PhysicalTableRef candidate(int discriminator) {
        return new PhysicalTableRef(
                "lake",
                InternalNamespace.SCHEMA_NAME,
                String.format("__pg_mat_%032x", discriminator + 1));
    }

    public static ExecutionFence fence() {
        return new ExecutionFence("anchor-1", 7, NOW.plus(Duration.ofMinutes(30)));
    }

    public static CommitEvidence evidence(String commitFingerprint) {
        return new CommitEvidence(
                "iceberg", commitFingerprint, "manifest-hash", "v2", NOW);
    }

    public static Generation candidateGeneration(
            MaterializationId object,
            GenerationId generationId,
            AttemptId attemptId,
            PhysicalTableRef candidate,
            RefreshKind kind,
            SourceVector vector,
            Optional<GenerationId> base) {
        return Generation.candidate(
                generationId,
                object,
                INCARNATION,
                1,
                attemptId,
                base,
                candidate,
                kind,
                vector,
                NOW);
    }

    public static RefreshAttempt attempt(
            MaterializationId object,
            AttemptId attemptId,
            PhysicalTableRef candidate,
            RefreshKind kind,
            SourceVector vector,
            Optional<GenerationId> base) {
        return RefreshAttempt.reserve(
                attemptId,
                header(object),
                definition(object, vector),
                REQUESTER,
                new RefreshAttempt.AttemptScope(
                        base, kind, vector, TriggerKind.MANUAL, fence(), candidate),
                NOW);
    }

    public static Dependency dependency(MaterializationId upstream, MaterializationId downstream, long revision) {
        return new Dependency(upstream, downstream, revision, DependencyKind.DIRECT);
    }

    /** Registry with the production built-in strategies and no rewrite participation. */
    public static MaterializationExtensionRegistry extensions() {
        return MaterializationExtensionRegistry.discover(MaterializationTestFixtures.class.getClassLoader());
    }

    /** Records every pin and unpin so tests can assert exact holds and releases. */
    public static final class RecordingPins implements SourcePinning {

        private final List<PinnedSource> pinned = new java.util.ArrayList<>();
        private final List<PinnedSource> released = new java.util.ArrayList<>();
        private final List<ChangeSetPlan> changeSets = new java.util.ArrayList<>();
        private RuntimeException pinFailure;
        private RuntimeException unpinFailure;
        private RuntimeException changeSetFailure;
        private java.util.List<org.pangrova.connector.api.materialization.PartitionChange> partitionChanges =
                java.util.List.of();
        private int failingPinCall = -1;
        private int pinCalls;
        private java.util.function.UnaryOperator<PinnedSource> pinResponse = source -> source;

        public List<PinnedSource> pinned() {
            return List.copyOf(pinned);
        }

        public List<PinnedSource> released() {
            return List.copyOf(released);
        }

        public List<ChangeSetPlan> changeSets() {
            return List.copyOf(changeSets);
        }

        public void failNextPin(RuntimeException failure) {
            this.pinFailure = failure;
        }

        public void failChangeSet(RuntimeException failure) {
            this.changeSetFailure = failure;
        }

        /** Fails the one-based pin call while every other pin succeeds. */
        public void failPinOnCall(int call, RuntimeException failure) {
            this.failingPinCall = call;
            this.pinFailure = failure;
        }

        public void failUnpin(RuntimeException failure) {
            this.unpinFailure = failure;
        }

        public void recoverUnpins() {
            this.unpinFailure = null;
        }

        public void respondWith(java.util.function.UnaryOperator<PinnedSource> response) {
            this.pinResponse = response;
        }

        /** Makes subsequent partition-replace change sets report these partitions. */
        public void respondWithPartitions(
                java.util.List<org.pangrova.connector.api.materialization.PartitionChange> changes) {
            this.partitionChanges = java.util.List.copyOf(changes);
        }

        @Override
        public PinnedSource pin(String tenant, SourceRef source, SourceSnapshotRef expected,
                String pinOwner) {
            pinCalls++;
            if (pinFailure != null && (failingPinCall < 0 || failingPinCall == pinCalls)) {
                RuntimeException failure = pinFailure;
                pinFailure = null;
                throw failure;
            }
            PinnedSource result = pinResponse.apply(
                    new PinnedSource(tenant, source, expected, pinOwner));
            pinned.add(result);
            return result;
        }

        @Override
        public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
            if (changeSetFailure != null) {
                RuntimeException failure = changeSetFailure;
                changeSetFailure = null;
                throw failure;
            }
            ChangeSetPlan plan = new ChangeSetPlan(from.source(), from.snapshot(), to.snapshot(),
                    mode, "change-set-" + changeSets.size(), NOW,
                    mode == ChangeSetMode.PARTITION_REPLACE ? partitionChanges : java.util.List.of());
            changeSets.add(plan);
            return plan;
        }

        @Override
        public ChangeProof verify(ChangeSetPlan plan) {
            return new ChangeProof(plan.source(), plan.fromSnapshot().snapshotToken(),
                    plan.toSnapshot().snapshotToken(), plan.mode(),
                    "proof-" + plan.planFingerprint(), "v1", NOW, true);
        }

        @Override
        public void unpin(PinnedSource source) {
            if (unpinFailure != null) {
                throw unpinFailure;
            }
            released.add(source);
        }
    }

    /** Fails every pin exactly like a connector without snapshot-retention support. */
    public static SourcePinning unpinnableSources() {
        return new SourcePinning() {
            @Override
            public PinnedSource pin(String tenant, SourceRef source, SourceSnapshotRef expected,
                    String pinOwner) {
                throw new MaterializationSourceException(
                        MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE,
                        "snapshot " + expected.snapshotToken() + " is not pinnable");
            }

            @Override
            public ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
                throw new MaterializationSourceException(
                        MaterializationSourceException.Reason.PIN_UNSUPPORTED,
                        "source does not support change sets");
            }

            @Override
            public ChangeProof verify(ChangeSetPlan plan) {
                throw new MaterializationSourceException(
                        MaterializationSourceException.Reason.PIN_UNSUPPORTED,
                        "source does not support change proofs");
            }

            @Override
            public void unpin(PinnedSource pinned) {
            }
        };
    }
}
