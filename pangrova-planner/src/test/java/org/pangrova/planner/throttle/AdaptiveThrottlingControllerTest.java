package org.pangrova.planner.throttle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class AdaptiveThrottlingControllerTest {

    private static final double INITIAL_RATE = 100.0;
    private static final double MIN_RATE = 1.0;
    private static final double MAX_RATE = 500.0;

    private AdaptiveThrottlingController createController() {
        var service = new QueryThrottlingService(10, INITIAL_RATE, INITIAL_RATE);
        return new AdaptiveThrottlingController(service, INITIAL_RATE, MIN_RATE, MAX_RATE);
    }

    @Test
    void highCpu_decreasesTenantRateBy20Percent() {
        var controller = createController();
        controller.adjust(new ClusterMetrics(0.95, 0.5, 0.05));
        assertThat(controller.currentTenantRate()).isEqualTo(INITIAL_RATE * 0.8);
    }

    @Test
    void highMemory_decreasesTenantRateBy20Percent() {
        var controller = createController();
        controller.adjust(new ClusterMetrics(0.5, 0.95, 0.05));
        assertThat(controller.currentTenantRate()).isEqualTo(INITIAL_RATE * 0.8);
    }

    @Test
    void highErrorRate_decreasesTenantRateBy20Percent() {
        var controller = createController();
        controller.adjust(new ClusterMetrics(0.5, 0.5, 0.15));
        assertThat(controller.currentTenantRate()).isEqualTo(INITIAL_RATE * 0.8);
    }

    @Test
    void allLow_increasesTenantRateBy10Percent() {
        var controller = createController();
        controller.adjust(new ClusterMetrics(0.3, 0.3, 0.005));
        assertThat(controller.currentTenantRate()).isEqualTo(INITIAL_RATE * 1.1);
    }

    @Test
    void moderateLoad_noChange() {
        var controller = createController();
        controller.adjust(new ClusterMetrics(0.6, 0.4, 0.05));
        assertThat(controller.currentTenantRate()).isEqualTo(INITIAL_RATE);
    }

    @Test
    void userRateIsHalfOfTenantRate() {
        var controller = createController();
        assertThat(controller.currentUserRate())
                .isCloseTo(INITIAL_RATE * AdaptiveThrottlingController.USER_RATE_RATIO, within(0.01));

        controller.adjust(new ClusterMetrics(0.95, 0.5, 0.05));
        double expectedTenant = INITIAL_RATE * 0.8;
        assertThat(controller.currentTenantRate()).isEqualTo(expectedTenant);
        assertThat(controller.currentUserRate())
                .isCloseTo(expectedTenant * AdaptiveThrottlingController.USER_RATE_RATIO, within(0.01));
    }

    @Test
    void tenantRateNeverDropsBelowMinimum() {
        var service = new QueryThrottlingService(10, 2.0, 2.0);
        var controller = new AdaptiveThrottlingController(service, 2.0, MIN_RATE, MAX_RATE);

        var overloaded = new ClusterMetrics(0.95, 0.95, 0.2);
        for (int i = 0; i < 50; i++) {
            controller.adjust(overloaded);
        }
        assertThat(controller.currentTenantRate()).isEqualTo(MIN_RATE);
    }

    @Test
    void tenantRateNeverExceedsMaximum() {
        var service = new QueryThrottlingService(10, 400.0, 400.0);
        var controller = new AdaptiveThrottlingController(service, 400.0, MIN_RATE, MAX_RATE);

        var underloaded = new ClusterMetrics(0.1, 0.1, 0.0);
        for (int i = 0; i < 50; i++) {
            controller.adjust(underloaded);
        }
        assertThat(controller.currentTenantRate()).isEqualTo(MAX_RATE);
    }
}
