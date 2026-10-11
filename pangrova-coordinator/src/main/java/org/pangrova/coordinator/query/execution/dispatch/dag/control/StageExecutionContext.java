package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;
import org.pangrova.metadata.artifact.jdbc.CurrentDeferredExecutionContext;
import org.pangrova.observability.tracing.ExecutionTraceContext;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Typed control authority for the shared stage data plane; deferred execution never impersonates a live user. */
@org.pangrova.spi.annotation.Internal
public sealed interface StageExecutionContext permits StageExecutionContext.Live,
        StageExecutionContext.Durable, StageExecutionContext.Deferred {
    String executionId();
    String tenantId();
    String principal();
    Set<String> activeRoles();
    Map<String, String> executionProperties();
    ExecutionTraceContext traceContext();

    /** Only a live query has a connector session; deferred authority cannot impersonate one. */
    default CoordinatorSessionContext requireLiveSession() {
        if (this instanceof Live live) {
            return live.session();
        }
        throw new IllegalStateException("Deferred stage execution has no authenticated live session capability");
    }

    record Live(CoordinatorSessionContext session) implements StageExecutionContext {
        public Live { Objects.requireNonNull(session, "session"); }
        public String executionId() { return session.queryId(); }
        public String tenantId() { return session.tenantId(); }
        public String principal() {
            return session.userIdentity() == null ? null : session.userIdentity().principal();
        }
        public Set<String> activeRoles() {
            return session.userIdentity() == null ? Set.of() : session.userIdentity().groups();
        }
        public Map<String, String> executionProperties() { return session.sessionProperties(); }
        public ExecutionTraceContext traceContext() { return session.traceContext(); }
    }

    /** Job-level authority resolved by the same current PG gate used for every exact partition claim. */
    record Durable(CurrentDeferredExecutionContext authority, Map<Integer, Long> allocationEpochs,
            ExecutionTraceContext traceContext)
            implements StageExecutionContext {
        public Durable {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(traceContext, "traceContext");
            allocationEpochs = Map.copyOf(allocationEpochs);
            for (var entry : allocationEpochs.entrySet()) {
                authority.dispatchSpec().stageGraph().requireStage(entry.getKey());
                if (entry.getValue() <= 0) throw new IllegalArgumentException("Allocation epoch must be positive");
            }
        }
        public Durable(CurrentDeferredExecutionContext authority, Map<Integer, Long> allocationEpochs) {
            this(authority, allocationEpochs, ExecutionTraceContext.capture());
        }
        /** Restored exchanges retain their original epoch, independently of the new access owner. */
        public long planEpoch(int stageId) {
            return allocationEpochs.getOrDefault(stageId, authority.executionAnchor().fencingToken());
        }
        public String executionId() { return authority.executionAnchor().executionId(); }
        public String tenantId() { return authority.dispatchSpec().tenantId(); }
        public String principal() { return authority.dispatchSpec().principal(); }
        public Set<String> activeRoles() { return authority.currentDirectRoles(); }
        public Map<String, String> executionProperties() {
            return Map.of("pangrova.policy_context_hash",
                    authority.dispatchSpec().authorizationScope().sha256());
        }
    }

    /** The private-constructor authority object can only be obtained from the current same-transaction PG resolver. */
    record Deferred(CurrentDeferredExecutionContext authority, ArtifactBuildLease lease, ExecutionTraceContext traceContext)
            implements StageExecutionContext {
        public Deferred {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(lease, "lease");
            Objects.requireNonNull(traceContext, "traceContext");
            var spec = authority.dispatchSpec();
            if (!spec.tenantId().equals(lease.tenantId()) || !spec.jobId().equals(lease.jobId())
                    || !authority.executionAnchor().executionId().equals(lease.executionAnchor().executionId())
                    || !ArtifactExecutionOwner.from(authority.executionAnchor()).equals(
                            ArtifactExecutionOwner.from(lease.executionAnchor()))) {
                throw new IllegalArgumentException(
                        "Deferred stage context differs from its actual leased execution owner");
            }
        }
        public Deferred(CurrentDeferredExecutionContext authority, ArtifactBuildLease lease) {
            this(authority, lease, ExecutionTraceContext.capture());
        }
        public String executionId() { return authority.executionAnchor().executionId(); }
        public String tenantId() { return authority.dispatchSpec().tenantId(); }
        public String principal() { return authority.dispatchSpec().principal(); }
        public Set<String> activeRoles() { return authority.currentDirectRoles(); }
        public Map<String, String> executionProperties() {
            return Map.of("pangrova.policy_context_hash",
                    authority.dispatchSpec().authorizationScope().sha256());
        }
    }
}
