package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcWatermarkPositionStoreTest {

    private DataSource dataSource;
    private JdbcWatermarkPositionStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("watermark");
        store = new JdbcWatermarkPositionStore(dataSource);
    }

    @Test
    void saveAndLoadRoundTrip() {
        Instant watermark = Instant.parse("2026-03-23T10:00:00Z");
        store.advance("orders", watermark);

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

        store.advance("orders", first);
        store.advance("orders", second);

        assertThat(store.load("orders")).hasValue(second);
    }

    @Test
    void restartedWriterCannotRegressTheStoredPosition() {
        Instant newer = Instant.parse("2026-03-23T11:00:00Z");
        assertThat(store.advance("orders", newer)).isEqualTo(newer);
        var restarted = new JdbcWatermarkPositionStore(dataSource);
        assertThat(restarted.advance("orders", newer.minusSeconds(60))).isEqualTo(newer);
        assertThat(store.load("orders")).contains(newer);
    }

    @Test
    void multipleSourcesAreIsolated() {
        Instant w1 = Instant.parse("2026-03-23T10:00:00Z");
        Instant w2 = Instant.parse("2026-03-23T12:00:00Z");

        store.advance("orders", w1);
        store.advance("events", w2);

        assertThat(store.load("orders")).hasValue(w1);
        assertThat(store.load("events")).hasValue(w2);
    }

    @Test
    void watermarkSurvivesMultipleUpdates() {
        for (int i = 0; i < 10; i++) {
            store.advance("orders", Instant.ofEpochSecond(1000 + i));
        }
        assertThat(store.load("orders")).hasValue(Instant.ofEpochSecond(1009));
    }
}
