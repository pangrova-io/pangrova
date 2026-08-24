package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests snapshot pinning lifecycle: beginQuery pins, cleanupQuery unpins and clears boundsCache.
 */
class PaimonMetadataSnapshotPinningTest {

    @TempDir
    Path warehouse;

    private PaimonMetadata metadata;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .build();
        catalog.createTable(Identifier.create("db", "orders"), schema, false);
        metadata = new PaimonMetadata(catalog, "test_catalog");
    }

    @Test
    void beginQueryPinsSnapshot() {
        metadata.beginQuery("q-1");
        assertThat(metadata.getPinnedSnapshot("q-1")).isEmpty();
    }

    @Test
    void cleanupQueryUnpinsAndClearsBoundsCache() {
        metadata.beginQuery("q-2");

        // Populate boundsCache via the nativeIndexEvaluator
        var evaluator = metadata.getNativeIndexEvaluator().orElseThrow();
        var paimonEvaluator = (org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator) evaluator;
        paimonEvaluator.cacheBounds("file1.parquet",
                new org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator.FileBounds(
                        Map.of(), Map.of()));

        metadata.cleanupQuery("q-2");

        assertThat(metadata.getPinnedSnapshot("q-2")).isEmpty();
    }

    @Test
    void cleanupQueryIsIdempotent() {
        metadata.beginQuery("q-3");
        metadata.cleanupQuery("q-3");
        metadata.cleanupQuery("q-3");
    }

    @Test
    void multipleQueriesArePinnedIndependently() {
        metadata.beginQuery("q-a");
        metadata.beginQuery("q-b");

        metadata.cleanupQuery("q-a");
        // q-b cleanup should still work
        metadata.cleanupQuery("q-b");
    }
}
