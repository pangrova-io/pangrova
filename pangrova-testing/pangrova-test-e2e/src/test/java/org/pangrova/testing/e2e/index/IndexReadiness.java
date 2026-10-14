package org.pangrova.testing.e2e.index;

import org.pangrova.testing.e2e.runner.SqlTestRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Waits for the asynchronous initial build before testing online REBUILD. */
public final class IndexReadiness {
    private IndexReadiness() { }

    public static void awaitReady(SqlTestRunner runner, String table, String indexName) {
        await("initial index READY: " + indexName)
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(runner.execute("SHOW INDEXES FROM " + table).stream()
                        .filter(row -> indexName.equals(row.getRaw("index_name").toString()))
                        .findFirst().orElseThrow().getRaw("state").toString()).isEqualTo("READY"));
    }
}
