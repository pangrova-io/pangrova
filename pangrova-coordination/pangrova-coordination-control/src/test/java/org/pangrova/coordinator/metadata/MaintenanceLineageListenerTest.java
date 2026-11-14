package org.pangrova.coordinator.metadata;

import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.metadata.api.enums.ObjectKind;
import org.pangrova.metadata.api.relation.ObjectRef;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class MaintenanceLineageListenerTest {
    private static final ObjectRef CHILD = new ObjectRef("catalog", "schema", "child", ObjectKind.VIEW);
    private static final ObjectRef PARENT = new ObjectRef("catalog", "schema", "parent", ObjectKind.TABLE);

    @Test
    void dependencyCompletionCarriesTheActualInputsAndOutput() {
        var event = terminal(hook -> hook.onDependenciesChanged(CHILD, Set.of(PARENT)));
        assertThat(event.inputDatasets()).containsExactly(dataset(PARENT));
        assertThat(event.outputDatasets()).containsExactly(dataset(CHILD));
        assertThat(event.sql()).contains("DEPENDENCIES_CHANGED");
    }

    @Test
    void dropCompletionNamesOnlyItsOwnObjectWithoutReplayingCascadeContext() {
        var event = terminal(hook -> hook.onObjectDropped(PARENT, List.of(CHILD)));
        assertThat(event.inputDatasets()).isEmpty();
        assertThat(event.outputDatasets()).containsExactly(dataset(PARENT));
        assertThat(event.sql()).contains("DROP");
    }

    @Test
    void snapshotCompletionRetainsTheObjectAndExpiredSnapshot() {
        var event = terminal(hook -> hook.onSnapshotExpired(PARENT, 42));
        assertThat(event.outputDatasets()).containsExactly(dataset(PARENT));
        assertThat(event.sql()).contains("snapshot=42");
    }

    private static DatasetRef dataset(ObjectRef ref) {
        return new DatasetRef(ref.catalog() + "." + ref.schema(), ref.name(), List.of());
    }

    private static LineageEvent terminal(Consumer<MaintenanceLineageListener> action) {
        var events = new CopyOnWriteArrayList<LineageEvent>();
        var backend = new LineageBackend() {
            @Override
            public void send(LineageEvent event) { events.add(event); }
            @Override
            public void close() {}
        };
        try (var emitter = new LineageEventEmitter(backend)) {
            action.accept(new MaintenanceLineageListener(emitter));
        }
        assertThat(events).extracting(LineageEvent::eventType)
                .containsExactly(LineageEvent.EventType.START, LineageEvent.EventType.COMPLETE);
        assertThat(events.get(0).runId()).isEqualTo(events.get(1).runId());
        return events.get(1);
    }
}
