package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.query.api.state.StateJournalConfig;

/**
 * Unit tests for {@link StateJournalConfig} record validation and defaults.
 */
class StateJournalConfigTest {

    @Test
    void noneDefaultValues() {
        var config = StateJournalConfig.NONE;
        assertEquals("NONE", config.type());
        assertNull(config.jdbcUrl());
        assertEquals(100, config.flushIntervalMs());
        assertEquals(50, config.batchSize());
        assertEquals(24, config.retentionHours());
        assertEquals(50_000, config.maxResidentEvents());
    }

    @Test
    void jdbcConfigCreation() {
        var config = new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 200, 100, 48, 50_000);
        assertEquals("JDBC", config.type());
        assertEquals("jdbc:postgresql://localhost:5432/pangrova", config.jdbcUrl());
        assertEquals(200, config.flushIntervalMs());
        assertEquals(100, config.batchSize());
        assertEquals(48, config.retentionHours());
    }

    @Test
    void nullTypeRejected() {
        assertThrows(NullPointerException.class,
                () -> new StateJournalConfig(null, null, 100, 50, 24, 50_000));
    }

    @Test
    void zeroFlushIntervalRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 0, 50, 24, 50_000));
    }

    @Test
    void negativeFlushIntervalRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", -1, 50, 24, 50_000));
    }

    @Test
    void zeroBatchSizeRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 0, 24, 50_000));
    }

    @Test
    void zeroRetentionHoursRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 50, 0, 50_000));
    }

    @Test
    void nullJdbcUrlAllowedForNoneType() {
        var config = new StateJournalConfig("NONE", null, 100, 50, 24, 50_000);
        assertNull(config.jdbcUrl());
    }

    @Test
    void equalityAndHashCode() {
        var a = new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 50, 24, 50_000);
        var b = new StateJournalConfig("JDBC", "jdbc:postgresql://localhost:5432/pangrova", 100, 50, 24, 50_000);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void nonPositiveResidentEventLimitRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("NONE", null, 100, 50, 24, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new StateJournalConfig("NONE", null, 100, 50, 24, -1));
    }
}
