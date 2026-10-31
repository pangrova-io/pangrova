package org.pangrova.connector.spi.metadata;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Result of a pushdown attempt, aligned with the Trino {@code XxxApplicationResult} contract.
 *
 * <p>Carries the rewritten connector handle plus a {@code guaranteed} flag telling the engine
 * whether the connector <em>fully</em> enforces the pushed-down operation:</p>
 * <ul>
 *   <li>{@code guaranteed == true} — the connector completely applies the operation, so the
 *       engine may remove the corresponding operator (LIMIT, TopN, aggregation, ...) from the
 *       plan.</li>
 *   <li>{@code guaranteed == false} — the connector applies the operation only partially (e.g.
 *       as a best-effort early-stop), so the engine must retain the operator as a residual over
 *       the connector's output to preserve correctness.</li>
 * </ul>
 *
 * <p>Returning {@code Optional.empty()} from a pushdown method (rather than a non-guaranteed
 * result that re-wraps the same handle) signals "no change" and prevents optimizer loops.</p>
 *
 * @param <H> the connector handle type carried by the result
 */
@Experimental
public record ApplicationResult<H>(H handle, boolean guaranteed) {

    public ApplicationResult {
        Objects.requireNonNull(handle, "handle must not be null");
    }

    /** The connector fully enforces the operation; the engine may drop the plan operator. */
    public static <H> ApplicationResult<H> guaranteed(H handle) {
        return new ApplicationResult<>(handle, true);
    }

    /** The connector applies the operation partially; the engine must keep a residual operator. */
    public static <H> ApplicationResult<H> residual(H handle) {
        return new ApplicationResult<>(handle, false);
    }
}
