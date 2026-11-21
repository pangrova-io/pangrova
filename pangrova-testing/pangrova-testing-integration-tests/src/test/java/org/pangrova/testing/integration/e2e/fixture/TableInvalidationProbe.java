package org.pangrova.testing.integration.e2e.fixture;

import org.awaitility.Awaitility;
import org.pangrova.metadata.api.enums.InvalidationType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises independent outbox writer and cache reader connections for an observed table. */
public final class TableInvalidationProbe {
    private TableInvalidationProbe() {}

    public static void verify(DataSource source, String catalog, String schema, String table) {
        var cache = new TieredMetadataCache();
        var observation = new TableMetadata(catalog, schema, table, List.of(), Map.of(), List.of(),
                Map.of(), Instant.now(), Instant.now());
        cache.putTable(catalog, schema, table, observation);
        assertThat(cache.getTable(catalog, schema, table)).contains(observation);

        try (var reader = new PostgresInvalidationPoller(source, cache, 1);
             var writer = new PostgresInvalidationPoller(source, new TieredMetadataCache())) {
            reader.validateSchema();
            writer.validateSchema();
            reader.start();
            writer.writeInvalidation(String.join("\u001f", catalog, schema, table), InvalidationType.TABLE);
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(
                    () -> cache.getTable(catalog, schema, table).isEmpty());
        }
    }
}
