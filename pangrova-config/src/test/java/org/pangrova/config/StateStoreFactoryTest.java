package org.pangrova.config;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.function.Function;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link StateStoreFactory}.
 * Validates registration, creation dispatch, and error handling per R8.2.
 */
class StateStoreFactoryTest {

    @AfterEach
    void tearDown() {
        StateStoreFactory.resetForTesting();
    }

    // ---- Registration ----

    @Test
    void registerAndCreateInMemory() {
        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                ds -> "in-memory-instance");

        CharSequence result = StateStoreFactory.create(
                CharSequence.class, StateStoreType.IN_MEMORY, null);

        assertThat(result).isEqualTo("in-memory-instance");
    }

    @Test
    void registerAndCreateJdbc() {
        DataSource ds = new StubDataSource();
        StateStoreFactory.register(CharSequence.class, StateStoreType.JDBC,
                dataSource -> "jdbc-instance");

        CharSequence result = StateStoreFactory.create(
                CharSequence.class, StateStoreType.JDBC, ds);

        assertThat(result).isEqualTo("jdbc-instance");
    }

    @Test
    void createPassesDataSourceToCreator() {
        DataSource ds = new StubDataSource();
        StateStoreFactory.register(CharSequence.class, StateStoreType.JDBC,
                dataSource -> {
                    assertThat(dataSource).isSameAs(ds);
                    return "from-ds";
                });

        CharSequence result = StateStoreFactory.create(
                CharSequence.class, StateStoreType.JDBC, ds);

        assertThat(result).isEqualTo("from-ds");
    }

    // ---- Error cases ----

    @Test
    void createThrowsWhenNoCreatorRegistered() {
        assertThatThrownBy(() ->
                StateStoreFactory.create(Runnable.class, StateStoreType.IN_MEMORY, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No state store registered")
                .hasMessageContaining("Runnable")
                .hasMessageContaining("IN_MEMORY");
    }

    @Test
    void createThrowsWhenJdbcWithoutDataSource() {
        StateStoreFactory.register(CharSequence.class, StateStoreType.JDBC,
                ds -> "jdbc-instance");

        assertThatThrownBy(() ->
                StateStoreFactory.create(CharSequence.class, StateStoreType.JDBC, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DataSource is required");
    }

    @Test
    void registerRejectsNullSpiType() {
        assertThatThrownBy(() ->
                StateStoreFactory.register(null, StateStoreType.IN_MEMORY, ds -> "x"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void registerRejectsNullStoreType() {
        assertThatThrownBy(() ->
                StateStoreFactory.register(CharSequence.class, null, ds -> "x"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void registerRejectsNullCreator() {
        assertThatThrownBy(() ->
                StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void createRejectsNullSpiType() {
        assertThatThrownBy(() ->
                StateStoreFactory.create(null, StateStoreType.IN_MEMORY, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void createRejectsNullStoreType() {
        assertThatThrownBy(() ->
                StateStoreFactory.create(CharSequence.class, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- Duplicate registration ----

    @Test
    void duplicateRegistrationFailsFast() {
        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                ds -> "first");

        // Prod contract: registry is fail-fast on conflicting (spiType, storeType) — must throw,
        // not silently overwrite or silently ignore. Verifies first registration is preserved
        // by re-creating and asserting the original creator still wins.
        assertThatThrownBy(() ->
                StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                        ds -> "second"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Conflicting StateStore registration")
                .hasMessageContaining("CharSequence")
                .hasMessageContaining("IN_MEMORY");

        CharSequence result = StateStoreFactory.create(
                CharSequence.class, StateStoreType.IN_MEMORY, null);
        assertThat(result)
                .as("first registration must remain authoritative after rejected duplicate")
                .isEqualTo("first");
    }

    @Test
    void idempotentRegistrationWithSameCreatorSucceeds() {
        Function<DataSource, CharSequence> creator = ds -> "shared";

        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY, creator);
        // Re-registering the exact same creator instance must not throw.
        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY, creator);

        CharSequence result = StateStoreFactory.create(
                CharSequence.class, StateStoreType.IN_MEMORY, null);
        assertThat(result).isEqualTo("shared");
    }

    // ---- Registry count ----

    @Test
    void registeredCountTracksEntries() {
        assertThat(StateStoreFactory.registeredCount()).isZero();

        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                ds -> "a");
        assertThat(StateStoreFactory.registeredCount()).isEqualTo(1);

        StateStoreFactory.register(CharSequence.class, StateStoreType.JDBC,
                ds -> "b");
        assertThat(StateStoreFactory.registeredCount()).isEqualTo(2);
    }

    @Test
    void resetClearsAllRegistrations() {
        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                ds -> "a");
        assertThat(StateStoreFactory.registeredCount()).isEqualTo(1);

        StateStoreFactory.resetForTesting();
        assertThat(StateStoreFactory.registeredCount()).isZero();
    }

    // ---- Different SPI types are independent ----

    @Test
    void differentSpiTypesAreIndependent() {
        StateStoreFactory.register(CharSequence.class, StateStoreType.IN_MEMORY,
                ds -> "charseq");
        StateStoreFactory.register(Comparable.class, StateStoreType.IN_MEMORY,
                ds -> 42);

        assertThat(StateStoreFactory.create(CharSequence.class, StateStoreType.IN_MEMORY, null))
                .isEqualTo("charseq");
        assertThat(StateStoreFactory.create(Comparable.class, StateStoreType.IN_MEMORY, null))
                .isEqualTo(42);
    }

    /**
     * Minimal DataSource stub for testing. No actual JDBC connections are created.
     */
    private static final class StubDataSource implements DataSource {
        @Override public Connection getConnection() throws SQLException { throw new UnsupportedOperationException(); }
        @Override public Connection getConnection(String u, String p) throws SQLException { throw new UnsupportedOperationException(); }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException { throw new SQLFeatureNotSupportedException(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
