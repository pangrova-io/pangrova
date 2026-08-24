package org.pangrova.spi.indexing.feature;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link FeatureStore} SPI contract using a minimal stub
 * implementation to verify the interface is well-formed and usable.
 */
class FeatureStoreContractTest {

    private static final Instant T1 = Instant.parse("2026-06-01T10:00:00Z");

    /**
     * Minimal stub implementation of FeatureStore for contract testing.
     */
    private static final FeatureStore STUB = new FeatureStore() {

        @Override
        public Optional<FeatureValue> getFeatures(String entityId, String featureGroup,
                                                   Instant asOfTimestamp) {
            if ("known-entity".equals(entityId)) {
                return Optional.of(new FeatureValue(
                        entityId, featureGroup,
                        Map.of("score", 0.95), asOfTimestamp));
            }
            return Optional.empty();
        }

        @Override
        public Map<String, FeatureValue> getFeaturesBatch(List<String> entityIds,
                                                           String featureGroup,
                                                           Instant asOfTimestamp) {
            var result = new java.util.HashMap<String, FeatureValue>();
            for (String id : entityIds) {
                getFeatures(id, featureGroup, asOfTimestamp)
                        .ifPresent(fv -> result.put(id, fv));
            }
            return Map.copyOf(result);
        }

        @Override
        public List<FeatureGroup> listFeatureGroups() {
            return List.of(new FeatureGroup(
                    "test_group", "Test features", "entity_id",
                    List.of("score"), "event_time"));
        }

        @Override
        public String storeName() {
            return "test-store";
        }
    };

    @Test
    void getFeaturesReturnsValueForKnownEntity() {
        Optional<FeatureValue> result = STUB.getFeatures("known-entity", "fg", T1);
        assertTrue(result.isPresent());
        assertEquals("known-entity", result.get().entityId());
        assertEquals(0.95, result.get().values().get("score"));
    }

    @Test
    void getFeaturesReturnsEmptyForUnknownEntity() {
        Optional<FeatureValue> result = STUB.getFeatures("unknown", "fg", T1);
        assertTrue(result.isEmpty());
    }

    @Test
    void getFeaturesBatchReturnsOnlyKnownEntities() {
        Map<String, FeatureValue> result = STUB.getFeaturesBatch(
                List.of("known-entity", "unknown-entity"), "fg", T1);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("known-entity"));
    }

    @Test
    void listFeatureGroupsReturnsNonEmpty() {
        List<FeatureGroup> groups = STUB.listFeatureGroups();
        assertNotNull(groups);
        assertEquals(1, groups.size());
        assertEquals("test_group", groups.get(0).name());
    }

    @Test
    void storeNameIsNonNull() {
        assertNotNull(STUB.storeName());
        assertEquals("test-store", STUB.storeName());
    }
}
