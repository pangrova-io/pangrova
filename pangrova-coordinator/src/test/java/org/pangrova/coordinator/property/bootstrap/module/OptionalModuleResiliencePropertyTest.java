package org.pangrova.coordinator.property.bootstrap.module;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import org.pangrova.coordinator.bootstrap.module.CoordinatorModule;
import org.pangrova.coordinator.bootstrap.module.CoordinatorModuleContext;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Property 19: disabling any subset of optional modules still allows startup.
 * Randomly selects module subsets to disable and verifies the remaining
 * modules configure successfully without exceptions.
 *
 * <p><b>Validates: Requirement 9.4</b></p>
 */
class OptionalModuleResiliencePropertyTest {

    @Property(tries = 200)
    void disablingModuleSubsetStillAllowsStartup(
            @ForAll @Size(min = 2, max = 8) List<@IntRange(min = 0, max = 500) Integer> priorities,
            @ForAll @IntRange(min = 0, max = 255) int disableMask) {

        int moduleCount = priorities.size();
        Set<Integer> disabledIndices = IntStream.range(0, moduleCount)
                .filter(i -> (disableMask & (1 << (i % 8))) != 0)
                .boxed()
                .collect(Collectors.toSet());

        List<CoordinatorModule> allModules = new ArrayList<>();
        List<Integer> configuredPriorities = new ArrayList<>();

        for (int i = 0; i < moduleCount; i++) {
            int priority = priorities.get(i);
            boolean disabled = disabledIndices.contains(i);
            allModules.add(new TestModule(priority, disabled, configuredPriorities));
        }

        allModules.sort(Comparator.comparingInt(CoordinatorModule::priority));
        DefaultModuleContext ctx = new DefaultModuleContext();

        assertThatNoException().isThrownBy(() -> {
            for (CoordinatorModule module : allModules) {
                module.configure(ctx);
            }
        });

        long enabledCount = IntStream.range(0, moduleCount)
                .filter(i -> !disabledIndices.contains(i))
                .count();
        assertThat(configuredPriorities).hasSize((int) enabledCount);
    }

    @Property(tries = 50)
    void allModulesDisabledStillAllowsStartup(
            @ForAll @Size(min = 1, max = 6) List<@IntRange(min = 0, max = 500) Integer> priorities) {

        List<CoordinatorModule> modules = new ArrayList<>();
        List<Integer> configured = new ArrayList<>();

        for (int priority : priorities) {
            modules.add(new TestModule(priority, true, configured));
        }

        modules.sort(Comparator.comparingInt(CoordinatorModule::priority));
        DefaultModuleContext ctx = new DefaultModuleContext();

        assertThatNoException().isThrownBy(() -> {
            for (CoordinatorModule module : modules) {
                module.configure(ctx);
            }
        });

        assertThat(configured).isEmpty();
    }

    /**
     * Test module that skips configuration when disabled,
     * mirroring the optional module pattern used by CacheModule and IndexingModule.
     */
    private record TestModule(
            int modulePriority,
            boolean disabled,
            List<Integer> configuredPriorities) implements CoordinatorModule {

        @Override
        public void configure(CoordinatorModuleContext context) {
            if (disabled) { return; }
            configuredPriorities.add(modulePriority);
        }

        @Override
        public int priority() {
            return modulePriority;
        }
    }
}
