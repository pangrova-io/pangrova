package org.pangrova.streaming.jdbc;


import org.pangrova.streaming.testinfra.CdcCheckpointDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcWatermarkStoreTest {

    private DataSource dataSource;
    private JdbcWatermarkStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = CdcCheckpointDatabase.isolatedStreamingDataSource();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS watermark_positions");
            stmt.execute("CREATE TABLE watermark_positions ("
                    + "source_id VARCHAR(512) NOT NULL PRIMARY KEY, "
                    + "position TIMESTAMP NOT NULL, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
        }
        store = new JdbcWatermarkStore(dataSource);
    }

    @Test
    void saveAndLoadRoundTrip() {
        Instant watermark = Instant.parse("2026-03-23T10:00:00Z");
        store.save("orders", watermark);

        Optional<Instant> loaded = store.load("orders");
        assertThat(loaded).isPresent().hasValue(watermark);
    }

    @Test
    void loadReturnsEmptyForUnknownSource() {
        assertThat(store.load("nonexistent")).isEmpty();
    }

    @Test
    void saveOverwritesPreviousValue() {
        Instant first = Instant.parse("2026-03-23T10:00:00Z");
        Instant second = Instant.parse("2026-03-23T11:00:00Z");

        store.save("orders", first);
        store.save("orders", second);

        assertThat(store.load("orders")).hasValue(second);
    }

    @Test
    void saveRejectsWatermarkRegression() {
        Instant newer = Instant.parse("2026-03-23T11:00:00Z");
        Instant older = Instant.parse("2026-03-23T10:00:00Z");

        store.save("orders", newer);
        store.save("orders", older);

        assertThat(store.load("orders")).hasValue(newer);
    }

    @Test
    void multipleSourcesAreIsolated() {
        Instant w1 = Instant.parse("2026-03-23T10:00:00Z");
        Instant w2 = Instant.parse("2026-03-23T12:00:00Z");

        store.save("orders", w1);
        store.save("events", w2);

        assertThat(store.load("orders")).hasValue(w1);
        assertThat(store.load("events")).hasValue(w2);
    }

    @Test
    void watermarkSurvivesMultipleUpdates() {
        for (int i = 0; i < 10; i++) {
            store.save("orders", Instant.ofEpochSecond(1000 + i));
        }
        assertThat(store.load("orders")).hasValue(Instant.ofEpochSecond(1009));
    }
}
