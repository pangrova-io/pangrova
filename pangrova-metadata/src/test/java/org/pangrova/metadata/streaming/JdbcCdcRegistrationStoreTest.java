package org.pangrova.metadata.streaming;

import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.streaming.CdcRegistration;
import org.pangrova.spi.streaming.CdcRegistration.CdcStreamState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcCdcRegistrationStoreTest {

    private JdbcCdcRegistrationStore store;

    @BeforeEach
    void setUp() throws Exception {
        store = new JdbcCdcRegistrationStore(SharedPostgresContainer.isolatedDataSource("cdc_registrations"));
        store.initSchema();
    }

    @Test
    void registerAndGetRoundTrip() {
        Instant now = Instant.now();
        CdcRegistration reg = new CdcRegistration(
                "stream-1", "consumer-a", "tenant-x",
                CdcStreamState.ACTIVE, 4, 1024L, now, now);

        store.register(reg);

        Optional<CdcRegistration> loaded = store.getRegistration("stream-1");
        assertThat(loaded).isPresent();
        assertThat(loaded.get().streamId()).isEqualTo("stream-1");
        assertThat(loaded.get().consumerId()).isEqualTo("consumer-a");
        assertThat(loaded.get().tenantId()).isEqualTo("tenant-x");
        assertThat(loaded.get().state()).isEqualTo(CdcStreamState.ACTIVE);
        assertThat(loaded.get().maxConcurrentWrites()).isEqualTo(4);
        assertThat(loaded.get().maxPendingBytes()).isEqualTo(1024L);
    }

    @Test
    void getRegistrationReturnsEmptyForMissing() {
        assertThat(store.getRegistration("nonexistent")).isEmpty();
    }

    @Test
    void registerUpsertOverwritesExisting() {
        Instant t1 = Instant.now();
        CdcRegistration original = new CdcRegistration(
                "stream-2", "consumer-b", null,
                CdcStreamState.ACTIVE, 2, 512L, t1, t1);
        store.register(original);

        Instant t2 = t1.plusSeconds(30);
        CdcRegistration updated = new CdcRegistration(
                "stream-2", "consumer-b", "tenant-y",
                CdcStreamState.PAUSED, 8, 2048L, t2, t1);
        store.register(updated);

        CdcRegistration loaded = store.getRegistration("stream-2").orElseThrow();
        assertThat(loaded.tenantId()).isEqualTo("tenant-y");
        assertThat(loaded.state()).isEqualTo(CdcStreamState.PAUSED);
        assertThat(loaded.maxConcurrentWrites()).isEqualTo(8);
        assertThat(loaded.maxPendingBytes()).isEqualTo(2048L);
    }

    @Test
    void unregisterRemovesExisting() {
        Instant now = Instant.now();
        store.register(new CdcRegistration(
                "stream-3", "consumer-c", null,
                CdcStreamState.ACTIVE, 1, 256L, now, now));

        boolean removed = store.unregister("stream-3");
        assertThat(removed).isTrue();
        assertThat(store.getRegistration("stream-3")).isEmpty();
    }

    @Test
    void unregisterReturnsFalseForMissing() {
        assertThat(store.unregister("nonexistent")).isFalse();
    }

    @Test
    void listRegistrationsReturnsAll() {
        Instant now = Instant.now();
        store.register(new CdcRegistration(
                "s1", "c1", null, CdcStreamState.ACTIVE, 1, 100L, now, now));
        store.register(new CdcRegistration(
                "s2", "c2", "t1", CdcStreamState.PAUSED, 2, 200L, now, now));
        store.register(new CdcRegistration(
                "s3", "c3", null, CdcStreamState.EXPIRED, 3, 300L, now, now));

        List<CdcRegistration> all = store.listRegistrations();
        assertThat(all).hasSize(3);
        assertThat(all).extracting(CdcRegistration::streamId)
                .containsExactlyInAnyOrder("s1", "s2", "s3");
    }

    @Test
    void listRegistrationsReturnsEmptyWhenNone() {
        assertThat(store.listRegistrations()).isEmpty();
    }

    @Test
    void nullTenantIdPersistsCorrectly() {
        Instant now = Instant.now();
        store.register(new CdcRegistration(
                "stream-null-tenant", "consumer-d", null,
                CdcStreamState.ACTIVE, 1, 100L, now, now));

        CdcRegistration loaded = store.getRegistration("stream-null-tenant").orElseThrow();
        assertThat(loaded.tenantId()).isNull();
    }

    @Test
    void initSchemaIsIdempotent() {
        store.initSchema();

        Instant now = Instant.now();
        store.register(new CdcRegistration(
                "stream-idem", "consumer-e", null,
                CdcStreamState.ACTIVE, 1, 100L, now, now));
        assertThat(store.getRegistration("stream-idem")).isPresent();
    }
}
