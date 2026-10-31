package org.pangrova.coordinator.metadata;

import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.metadata.api.relation.MetadataMaintenanceHook;
import org.pangrova.metadata.api.relation.ObjectRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Bridges the metadata dependency graph to the governance lineage subsystem
 * by emitting OpenLineage-compatible events through {@link LineageEventEmitter}
 * whenever the dependency graph mutates.
 *
 * <p>Explicit consumers can register this adapter on an ObjectDependencyService.
 * Current coordinator bootstrap does not register it; query lineage uses its
 * own durable outbox. When explicitly invoked, this adapter emits:
 * <ul>
 *   <li>{@code onObjectDropped} — a COMPLETE event with the dropped object as
 *       an output dataset, recording the completed drop operation.</li>
 *   <li>{@code onDependenciesChanged} — a COMPLETE event with the child as
 *       output and parents as inputs, reflecting the new lineage edges.</li>
 *   <li>{@code onSnapshotExpired} — a COMPLETE event recording which table
 *       and snapshot were expired for auditability.</li>
 * </ul>
 */
public final class LineageBridgeMaintenanceHook implements MetadataMaintenanceHook {

    private static final Logger LOG = LoggerFactory.getLogger(LineageBridgeMaintenanceHook.class);

    private final LineageEventEmitter emitter;

    public LineageBridgeMaintenanceHook(LineageEventEmitter emitter) {
        this.emitter = Objects.requireNonNull(emitter, "emitter must not be null");
    }

    @Override
    public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
        String eventId = "dep-drop-" + UUID.randomUUID();
        DatasetRef dataset = toDatasetRef(ref);
        emitCompletedOperation(eventId, "DROP " + ref, List.of(), List.of(dataset));
        LOG.debug("event=lineage_bridge_drop_emitted ref={} cascaded={}", ref, cascadedDependents.size());
    }

    @Override
    public void onSnapshotExpired(ObjectRef ref, long snapshotId) {
        String eventId = "dep-expire-" + UUID.randomUUID();
        DatasetRef dataset = toDatasetRef(ref);
        emitCompletedOperation(eventId,
                "EXPIRE_SNAPSHOT " + ref + " snapshot=" + snapshotId,
                List.of(), List.of(dataset));
        LOG.debug("event=lineage_bridge_expire_emitted ref={} snapshotId={}", ref, snapshotId);
    }

    @Override
    public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
        String eventId = "dep-change-" + UUID.randomUUID();
        List<DatasetRef> inputs = newParents.stream()
                .map(LineageBridgeMaintenanceHook::toDatasetRef)
                .toList();
        DatasetRef output = toDatasetRef(child);
        emitCompletedOperation(eventId,
                "DEPENDENCIES_CHANGED " + child,
                inputs, List.of(output));
        LOG.debug("event=lineage_bridge_deps_changed_emitted child={} parents={}",
                child, newParents.size());
    }

    private void emitCompletedOperation(String eventId, String operation,
            List<DatasetRef> inputs, List<DatasetRef> outputs) {
        emitter.emitStartEvent(eventId, operation, inputs, outputs);
        emitter.emitTerminalEvent(LineageEvent.EventType.COMPLETE, eventId, operation, inputs, outputs, Map.of());
    }

    private static DatasetRef toDatasetRef(ObjectRef ref) {
        String namespace = ref.catalog() + "." + ref.schema();
        return new DatasetRef(namespace, ref.name(), List.of());
    }
}
