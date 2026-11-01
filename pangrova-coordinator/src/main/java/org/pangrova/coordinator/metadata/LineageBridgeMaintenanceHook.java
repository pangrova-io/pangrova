package org.pangrova.coordinator.metadata;

import org.pangrova.governance.lineage.DatasetRef;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.metadata.core.dependency.MetadataMaintenanceHook;
import org.pangrova.metadata.core.dependency.ObjectRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Bridges the metadata dependency graph to the governance lineage subsystem
 * by emitting OpenLineage-compatible events through {@link LineageEventEmitter}
 * whenever the dependency graph mutates.
 *
 * <p>This hook is registered on the {@link org.pangrova.metadata.core.dependency.ObjectDependencyService}
 * during coordinator bootstrap. It emits:
 * <ul>
 *   <li>{@code onObjectDropped} — a COMPLETE event with the dropped object as
 *       an output dataset, so external lineage catalogs (Marquez, DataHub)
 *       can mark it as deleted.</li>
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
        emitter.emitStartEvent(eventId, "DROP " + ref, List.of(), List.of(dataset));
        emitter.emitCompleteEvent(eventId);
        LOG.debug("event=lineage_bridge_drop_emitted ref={} cascaded={}", ref, cascadedDependents.size());
    }

    @Override
    public void onSnapshotExpired(ObjectRef ref, long snapshotId) {
        String eventId = "dep-expire-" + UUID.randomUUID();
        DatasetRef dataset = toDatasetRef(ref);
        emitter.emitStartEvent(eventId,
                "EXPIRE_SNAPSHOT " + ref + " snapshot=" + snapshotId,
                List.of(), List.of(dataset));
        emitter.emitCompleteEvent(eventId);
        LOG.debug("event=lineage_bridge_expire_emitted ref={} snapshotId={}", ref, snapshotId);
    }

    @Override
    public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
        String eventId = "dep-change-" + UUID.randomUUID();
        List<DatasetRef> inputs = newParents.stream()
                .map(LineageBridgeMaintenanceHook::toDatasetRef)
                .toList();
        DatasetRef output = toDatasetRef(child);
        emitter.emitStartEvent(eventId,
                "DEPENDENCIES_CHANGED " + child,
                inputs, List.of(output));
        emitter.emitCompleteEvent(eventId);
        LOG.debug("event=lineage_bridge_deps_changed_emitted child={} parents={}",
                child, newParents.size());
    }

    private static DatasetRef toDatasetRef(ObjectRef ref) {
        String namespace = ref.catalog() + "." + ref.schema();
        return new DatasetRef(namespace, ref.name(), List.of());
    }
}
