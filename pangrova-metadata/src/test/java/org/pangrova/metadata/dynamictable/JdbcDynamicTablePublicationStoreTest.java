package org.pangrova.metadata.dynamictable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcDynamicTablePublicationStoreTest extends ContainerTestBase {

    private static final String TENANT = "tenant-dynamic-table";
    private static final String NAME = "lake.analytics.daily_events";
    private static final String DEFINITION = "SELECT event_id FROM lake.raw.events";

    private DataSource dataSource;
    private JdbcDynamicTablePublicationStore store;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("dynamic_table_publication");
        store = new JdbcDynamicTablePublicationStore(dataSource);
    }

    @Test
    void definitionAndPublishedGenerationSurviveStoreReopen() {
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        DynamicTablePublication creating = store.load(TENANT, NAME).orElseThrow();
        assertThat(creating.state()).isEqualTo(DynamicTablePublication.State.CREATING);
        assertThat(creating.publishedTable()).isNull();

        store.publish(creating, "lake.analytics.__pg_dt_first");
        DynamicTablePublication published = new JdbcDynamicTablePublicationStore(dataSource)
                .load(TENANT, NAME).orElseThrow();
        assertThat(published.definitionSql()).isEqualTo(DEFINITION);
        assertThat(published.bindingCatalog()).isEqualTo("lake");
        assertThat(published.bindingSchema()).isEqualTo("raw");
        assertThat(published.publishedTable()).isEqualTo("lake.analytics.__pg_dt_first");
        assertThat(published.revision()).isEqualTo(1);
        assertThat(published.state()).isEqualTo(DynamicTablePublication.State.ACTIVE);
    }

    @Test
    void staleRefreshCannotReplacePublishedGeneration() {
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        DynamicTablePublication creating = store.load(TENANT, NAME).orElseThrow();
        store.publish(creating, "lake.analytics.__pg_dt_first");
        DynamicTablePublication first = store.load(TENANT, NAME).orElseThrow();
        store.publish(first, "lake.analytics.__pg_dt_second");

        assertThatThrownBy(() -> store.publish(first, "lake.analytics.__pg_dt_stale"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changed during refresh");
        assertThat(store.load(TENANT, NAME).orElseThrow().publishedTable())
                .isEqualTo("lake.analytics.__pg_dt_second");
    }

    @Test
    void droppedNameCannotBeReusedByOldRefreshOrCleanup() {
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        DynamicTablePublication first = store.load(TENANT, NAME).orElseThrow();
        store.publish(first, "lake.analytics.__pg_dt_first");
        store.delete(store.load(TENANT, NAME).orElseThrow());
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        DynamicTablePublication second = store.load(TENANT, NAME).orElseThrow();

        assertThat(second.incarnationId()).isNotEqualTo(first.incarnationId());
        assertThatThrownBy(() -> store.publish(first, "lake.analytics.__pg_dt_stale"))
                .isInstanceOf(IllegalStateException.class);
        store.abandonCreation(first);
        assertThat(store.load(TENANT, NAME)).contains(second);
    }

    @Test
    void tenantIdentityIsolatesSameLogicalName() {
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        store.create("other-tenant", NAME, DEFINITION, "lake", "raw");
        DynamicTablePublication first = store.load(TENANT, NAME).orElseThrow();
        store.publish(first, "lake.analytics.__pg_dt_first");

        assertThat(store.load("other-tenant", NAME).orElseThrow().publishedTable()).isNull();
        assertThat(store.list(TENANT)).extracting(DynamicTablePublication::tenantId)
                .containsExactly(TENANT);
    }

    @Test
    void sameSimpleNameInDifferentSchemasHasIndependentHead() {
        String otherSchema = "lake.reporting.daily_events";
        store.create(TENANT, NAME, DEFINITION, "lake", "raw");
        store.create(TENANT, otherSchema, DEFINITION, "lake", "raw");
        store.publish(store.load(TENANT, NAME).orElseThrow(), "lake.analytics.__pg_dt_first");

        assertThat(store.load(TENANT, otherSchema).orElseThrow().publishedTable()).isNull();
        assertThat(store.list(TENANT)).extracting(DynamicTablePublication::logicalName)
                .containsExactlyInAnyOrder(NAME, otherSchema);
    }
}
