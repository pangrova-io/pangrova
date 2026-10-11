package org.pangrova.metadata.event;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaChange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataEventSnapshotTest {
    private static final QualifiedTableRef TABLE = new QualifiedTableRef("catalog", "schema", "table");
    private static final SchemaChange CHANGE = new SchemaChange.DropColumn("column");

    @Test
    void tableAlteredOwnsAnImmutableSnapshotOfTheSubmittedChanges() {
        var changes = new ArrayList<SchemaChange>(List.of(CHANGE));
        var event = new MetadataEvent.TableAlteredEvent(TABLE, changes);
        changes.clear();

        assertThat(event.changes()).containsExactly(CHANGE);
        assertThatThrownBy(() -> event.changes().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void schemaEvolutionOwnsAnImmutableSnapshotOfTheSubmittedChanges() {
        var changes = new ArrayList<SchemaChange>(List.of(CHANGE));
        var event = new MetadataEvent.SchemaEvolutionEvent("catalog", TABLE, changes,
                MetadataEvent.TenantImpactScope.global());
        changes.clear();

        assertThat(event.schemaChanges()).containsExactly(CHANGE);
        assertThatThrownBy(() -> event.schemaChanges().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
