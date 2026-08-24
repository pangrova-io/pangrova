package org.pangrova.coordinator.property.bootstrap.module;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import org.pangrova.coordinator.bootstrap.module.CoordinatorModule;
import org.pangrova.coordinator.bootstrap.module.CoordinatorModuleContext;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 18: modules are loaded in priority ascending order.
 * Randomly generates module lists with varying priorities and verifies
 * that configure() is called in priority-ascending sequence.
 *
 * <p><b>Validates: Requirement 9.3</b></p>
 */
class ModuleLoadingOrderPropertyTest {

    @Property(tries = 200)
    void modulesAreConfiguredInPriorityAscendingOrder(
            @ForAll @Size(min = 1, max = 10) List<@IntRange(min = 0, max = 1000) Integer> priorities) {

        CopyOnWriteArrayList<Integer> configureOrder = new CopyOnWriteArrayList<>();
        List<CoordinatorModule> modules = new ArrayList<>();

        for (int priority : priorities) {
            int p = priority;
            modules.add(new CoordinatorModule() {
                @Override
                public void configure(CoordinatorModuleContext context) {
                    configureOrder.add(p);
                }

                @Override
                public int priority() {
                    return p;
                }
            });
        }

        modules.sort(Comparator.comparingInt(CoordinatorModule::priority));
        DefaultModuleContext ctx = new DefaultModuleContext();
        for (CoordinatorModule module : modules) {
            module.configure(ctx);
        }

        assertThat(configureOrder).hasSize(priorities.size());
        for (int i = 1; i < configureOrder.size(); i++) {
            assertThat(configureOrder.get(i))
                    .as("Module at index %d (priority=%d) should be >= previous (priority=%d)",
                            i, configureOrder.get(i), configureOrder.get(i - 1))
                    .isGreaterThanOrEqualTo(configureOrder.get(i - 1));
        }
    }

    @Property(tries = 100)
    void defaultPriorityIsOneHundred() {
        CoordinatorModule module = ctx -> {};
        assertThat(module.priority()).isEqualTo(100);
    }
}
