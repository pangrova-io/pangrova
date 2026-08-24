package org.pangrova.planner.mv;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.RefreshStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaterializedViewRegistryTest {

    private MaterializedViewRegistry manager;

    @BeforeEach
    void setUp() {
        manager = new MaterializedViewRegistry();
    }

    @Test
    void registerAndGetDefinition() {
        MaterializedViewDefinition def = mvDefinition("mv1", RefreshStrategy.INCREMENTAL);
        manager.register(def);

        assertThat(manager.getDefinition("mv1")).isPresent();
        assertThat(manager.getDefinition("mv1").get().refreshStrategy())
                .isEqualTo(RefreshStrategy.INCREMENTAL);
    }

    @Test
    void getAllDefinitionsReturnsRegistered() {
        manager.register(mvDefinition("mv1", RefreshStrategy.INCREMENTAL));
        manager.register(mvDefinition("mv2", RefreshStrategy.INCREMENTAL));

        assertThat(manager.getAllDefinitions()).hasSize(2);
    }

    @Test
    void dropRemovesView() {
        manager.register(mvDefinition("mv6", RefreshStrategy.INCREMENTAL));
        manager.dropMaterializedView("mv6");

        assertThat(manager.getDefinition("mv6")).isEmpty();
    }

    @Test
    void dropNonExistentThrows() {
        assertThatThrownBy(() -> manager.dropMaterializedView("ghost"))
                .isInstanceOf(MaterializedViewException.class)
                .satisfies(e -> assertThat(((MaterializedViewException) e).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.MATERIALIZED_VIEW_CREATION_FAILURE));
    }

    @Test
    void getDefinitionReturnsEmptyForUnknown() {
        assertThat(manager.getDefinition("nonexistent")).isEmpty();
    }

    // --- helper ---

    private static MaterializedViewDefinition mvDefinition(String name, RefreshStrategy strategy) {
        return new MaterializedViewDefinition(
                name, "SELECT 1", strategy, true,
                Optional.of(Instant.now()), Instant.now());
    }
}
