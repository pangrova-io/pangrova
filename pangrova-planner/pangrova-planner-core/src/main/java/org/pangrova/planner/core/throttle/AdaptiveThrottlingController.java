package org.pangrova.planner.core.throttle;

/**
 * Adjusts query rate limits based on cluster resource usage.
 * Decrease by 20% when overloaded, increase by 10% when underloaded.
 * These percentages apply to the tenant rate; user rate tracks at
 * {@link #USER_RATE_RATIO} (50%) of the tenant rate.
 * Rates are clamped to [minRate, maxRate] to prevent shutdown or unbounded growth.
 *
 * <p>Tenant and user rates are adjusted independently with a fixed ratio:
 * user rate = tenant rate × {@link #USER_RATE_RATIO}. This prevents a single
 * user from consuming the entire tenant quota.</p>
 *
 * <p>Concurrency strategy: rate fields are volatile to ensure visibility
 * across threads. The {@link #adjust(ClusterMetrics)} method is expected to be
 * called by a single scheduler thread; concurrent calls are safe but may
 * interleave rate adjustments.</p>
 */
public class AdaptiveThrottlingController {

    private static final double DECREASE_FACTOR = 0.8;
    private static final double INCREASE_FACTOR = 1.1;

    /** User rate is 50% of tenant rate to prevent single-user monopolization. */
    static final double USER_RATE_RATIO = 0.5;

    private record RateSnapshot(double tenantRate, double userRate) {}

    private final QueryThrottlingService throttlingService;
    private final double minRate;
    private final double maxRate;
    private volatile RateSnapshot rates;

    /**
     * Creates a controller that adapts the accept rate within the given bounds.
     *
     * @param throttlingService throttling service providing success/accept rates
     * @param initialRate starting accept rate
     * @param minRate lower bound for the accept rate
     * @param maxRate upper bound for the accept rate
     */
    public AdaptiveThrottlingController(
            QueryThrottlingService throttlingService,
            double initialRate,
            double minRate,
            double maxRate) {
        this.throttlingService = throttlingService;
        this.minRate = minRate;
        this.maxRate = maxRate;
        this.rates = new RateSnapshot(clamp(initialRate), clamp(initialRate * USER_RATE_RATIO));
    }

    /**
     * Adjust rate limits based on current cluster metrics.
     * Overloaded → decrease 20%; underloaded → increase 10%; otherwise no change.
     */
    public void adjust(ClusterMetrics metrics) {
        RateSnapshot current = this.rates;
        double newTenantRate = current.tenantRate();
        if (metrics.isOverloaded()) {
            newTenantRate = clamp(newTenantRate * DECREASE_FACTOR);
        } else if (metrics.isUnderloaded()) {
            newTenantRate = clamp(newTenantRate * INCREASE_FACTOR);
        }
        double newUserRate = clamp(newTenantRate * USER_RATE_RATIO);
        // BUG-23 fix: apply to service first, then update snapshot.
        // Readers of currentTenantRate() will see the old value until
        // the service has actually applied the new rate.
        throttlingService.adjustRates(newTenantRate, newUserRate);
        this.rates = new RateSnapshot(newTenantRate, newUserRate);
    }

    public double currentTenantRate() {
        return rates.tenantRate();
    }

    public double currentUserRate() {
        return rates.userRate();
    }

    private double clamp(double rate) {
        return Math.max(minRate, Math.min(maxRate, rate));
    }
}
