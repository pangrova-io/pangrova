package org.pangrova.config;

import org.pangrova.config.enums.StateStoreType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StateStoreType}.
 * Validates enum values and string conversion per R8.2.
 */
class StateStoreTypeTest {

    @Test
    void enumContainsInMemoryAndJdbc() {
        assertThat(StateStoreType.values())
                .containsExactly(StateStoreType.IN_MEMORY, StateStoreType.JDBC);
    }

    @Test
    void valueOfInMemory() {
        assertThat(StateStoreType.valueOf("IN_MEMORY")).isEqualTo(StateStoreType.IN_MEMORY);
    }

    @Test
    void valueOfJdbc() {
        assertThat(StateStoreType.valueOf("JDBC")).isEqualTo(StateStoreType.JDBC);
    }
}
