package org.pangrova.planner.core.runtimefilter;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeFilterRegistryTest {

    private final RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

    @Test
    void register_andGet_returnsFilter() {
        RuntimeFilter filter = RuntimeFilter.minMax("col", 1, 100);
        registry.register("stage-1", filter);

        Optional<RuntimeFilter> result = registry.getFilter("stage-1");

        assertThat(result).isPresent().containsSame(filter);
    }

    @Test
    void getFilter_unknownStage_returnsEmpty() {
        assertThat(registry.getFilter("nonexistent")).isEmpty();
    }

    @Test
    void register_overwritesPreviousFilter() {
        RuntimeFilter first = RuntimeFilter.minMax("col", 1, 50);
        RuntimeFilter second = RuntimeFilter.minMax("col", 10, 200);
        registry.register("stage-1", first);
        registry.register("stage-1", second);

        assertThat(registry.getFilter("stage-1")).containsSame(second);
    }

    @Test
    void clear_removesAllFilters() {
        registry.register("s1", RuntimeFilter.minMax("a", 1, 10));
        registry.register("s2", RuntimeFilter.minMax("b", 5, 20));

        registry.clear();

        assertThat(registry.getFilter("s1")).isEmpty();
        assertThat(registry.getFilter("s2")).isEmpty();
    }
}
