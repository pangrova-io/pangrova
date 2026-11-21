package org.pangrova.governance.runtime.classification;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * G-META-06: validates catalog-level tag/classification governance.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Tag assignment, retrieval, and removal lifecycle</li>
 *   <li>Listener notification on tag changes</li>
 *   <li>Tag presence checking (hasTag)</li>
 *   <li>Startup replay of persisted tags</li>
 *   <li>Null argument validation</li>
 *   <li>Listener fault isolation</li>
 * </ul>
 */
class CatalogTagServiceTest {

    private InMemoryCatalogTagStore tagStore;
    private CatalogTagService service;

    @BeforeEach
    void setUp() {
        tagStore = new InMemoryCatalogTagStore();
        service = new CatalogTagService(tagStore);
    }

    @Test
    void assignTagPersistsAndReturns() {
        service.assignTag("warehouse", "classification", "pii");

        Map<String, String> tags = service.getTags("warehouse");
        assertThat(tags).containsEntry("classification", "pii");
    }

    @Test
    void assignTagOverwritesPreviousValue() {
        service.assignTag("warehouse", "environment", "staging");
        service.assignTag("warehouse", "environment", "production");

        Map<String, String> tags = service.getTags("warehouse");
        assertThat(tags).containsEntry("environment", "production");
        assertThat(tags).hasSize(1);
    }

    @Test
    void removeTagDeletesFromStore() {
        service.assignTag("warehouse", "classification", "pii");
        service.removeTag("warehouse", "classification");

        Map<String, String> tags = service.getTags("warehouse");
        assertThat(tags).isEmpty();
    }

    @Test
    void removeNonexistentTagIsNoOp() {
        // Should not throw
        service.removeTag("warehouse", "nonexistent");
        assertThat(service.getTags("warehouse")).isEmpty();
    }

    @Test
    void hasTagReturnsTrueWhenPresent() {
        service.assignTag("warehouse", "classification", "pii");
        assertThat(service.hasTag("warehouse", "classification", "pii")).isTrue();
    }

    @Test
    void hasTagReturnsFalseWhenAbsent() {
        assertThat(service.hasTag("warehouse", "classification", "pii")).isFalse();
    }

    @Test
    void hasTagReturnsFalseWhenValueDiffers() {
        service.assignTag("warehouse", "classification", "internal");
        assertThat(service.hasTag("warehouse", "classification", "pii")).isFalse();
    }

    @Test
    void listenerNotifiedOnAssign() {
        RecordingListener listener = new RecordingListener();
        service.addListener(listener);

        service.assignTag("warehouse", "classification", "pii");

        assertThat(listener.events).hasSize(1);
        RecordingListener.Event event = listener.events.get(0);
        assertThat(event.catalogName).isEqualTo("warehouse");
        assertThat(event.tagKey).isEqualTo("classification");
        assertThat(event.oldValue).isNull();
        assertThat(event.newValue).isEqualTo("pii");
    }

    @Test
    void listenerNotifiedOnOverwrite() {
        service.assignTag("warehouse", "tier", "dev");

        RecordingListener listener = new RecordingListener();
        service.addListener(listener);

        service.assignTag("warehouse", "tier", "critical");

        assertThat(listener.events).hasSize(1);
        RecordingListener.Event event = listener.events.get(0);
        assertThat(event.oldValue).isEqualTo("dev");
        assertThat(event.newValue).isEqualTo("critical");
    }

    @Test
    void listenerNotifiedOnRemove() {
        service.assignTag("warehouse", "classification", "pii");

        RecordingListener listener = new RecordingListener();
        service.addListener(listener);

        service.removeTag("warehouse", "classification");

        assertThat(listener.events).hasSize(1);
        RecordingListener.Event event = listener.events.get(0);
        assertThat(event.oldValue).isEqualTo("pii");
        assertThat(event.newValue).isNull();
    }

    @Test
    void removedListenerNotNotified() {
        RecordingListener listener = new RecordingListener();
        service.addListener(listener);
        service.removeListener(listener);

        service.assignTag("warehouse", "classification", "pii");

        assertThat(listener.events).isEmpty();
    }

    @Test
    void listenerFaultIsolation() {
        RecordingListener goodListener = new RecordingListener();
        service.addListener((catalog, tagKey, oldValue, newValue) -> {
            throw new RuntimeException("bad listener");
        });
        service.addListener(goodListener);

        service.assignTag("warehouse", "classification", "pii");

        // Good listener should still receive the event
        assertThat(goodListener.events).hasSize(1);
    }

    @Test
    void loadExistingTagsReplaysAllTags() {
        tagStore.saveTag("cat1", "classification", "pii");
        tagStore.saveTag("cat2", "environment", "production");

        RecordingListener listener = new RecordingListener();
        service.addListener(listener);

        int replayed = service.loadExistingTags();

        assertThat(replayed).isEqualTo(2);
        assertThat(listener.events).hasSize(2);
    }

    @Test
    void multipleCatalogsHaveIndependentTags() {
        service.assignTag("warehouse_a", "classification", "pii");
        service.assignTag("warehouse_b", "classification", "internal");

        assertThat(service.getTags("warehouse_a")).containsEntry("classification", "pii");
        assertThat(service.getTags("warehouse_b")).containsEntry("classification", "internal");
    }

    @Test
    void getTagsForUnknownCatalogReturnsEmpty() {
        assertThat(service.getTags("unknown")).isEmpty();
    }

    @Test
    void nullCatalogNameThrows() {
        assertThatThrownBy(() -> service.assignTag(null, "key", "value"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullTagKeyThrows() {
        assertThatThrownBy(() -> service.assignTag("cat", null, "value"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullTagValueThrows() {
        assertThatThrownBy(() -> service.assignTag("cat", "key", null))
                .isInstanceOf(NullPointerException.class);
    }

    // ── Test infrastructure ──

    private static final class RecordingListener
            implements CatalogTagService.CatalogTagChangedListener {

        final List<Event> events = new ArrayList<>();

        @Override
        public void onCatalogTagChanged(String catalogName, String tagKey,
                                        String oldValue, String newValue) {
            events.add(new Event(catalogName, tagKey, oldValue, newValue));
        }

        record Event(String catalogName, String tagKey, String oldValue, String newValue) {}
    }
}
