package org.pangrova.planner.rule;

/**
 * Contract by which a Calcite {@code Table} (registered behind a
 * {@link org.apache.calcite.plan.RelOptTable}) exposes connector pushdown
 * capability to planner rules.
 *
 * <p>The planner does not depend on the coordinator's metadata package, so the
 * concrete table implementation lives in {@code pangrova-coordinator}; the planner
 * rule discovers pushdown support via {@link org.apache.calcite.plan.RelOptTable#unwrap(Class)}
 * with this interface, and invokes the methods below to ask the connector to absorb
 * an operator.</p>
 *
 * <p>Each pushdown method returns a {@link PushdownResult}: when present, the table
 * has updated its internal connector handle and the rule should rewrite the plan;
 * the {@code guaranteed} flag tells the rule whether the connector fully enforces
 * the operation (so the rule may drop the corresponding plan operator) or only
 * partially (the rule must keep a residual operator over the connector's output).</p>
 */
public interface ConnectorPushdownTable {

    /**
     * Result of a pushdown attempt: the connector accepted the operation, and the
     * {@code guaranteed} flag tells the rule whether the engine may drop the plan
     * operator or must retain it as a residual.
     */
    record PushdownResult(boolean guaranteed) {
        public static final PushdownResult GUARANTEED = new PushdownResult(true);
        public static final PushdownResult RESIDUAL = new PushdownResult(false);
    }

    /**
     * Asks the connector to absorb a row limit.
     *
     * <p>Implementations should call {@code ConnectorMetadata.applyLimit} on the
     * connector's metadata, store the returned handle as the table's new connector
     * handle, and return {@link PushdownResult#GUARANTEED} if the connector strictly
     * caps the result to {@code limit} rows or {@link PushdownResult#RESIDUAL} if
     * the connector applies the limit only as a best-effort early-stop hint.</p>
     *
     * @param limit positive maximum number of rows the connector should produce
     * @return present if the connector accepted the limit; empty if not supported
     */
    java.util.Optional<PushdownResult> pushLimit(long limit);

    /**
     * Asks the connector to absorb an aggregation (COUNT/MIN/MAX/SUM with optional GROUP BY).
     *
     * <p>Connectors that can compute aggregates close to storage (manifest stats,
     * Puffin sketches, partition-level pre-aggregation) return a result. The
     * {@code guaranteed} flag tells the rule whether the engine can drop the
     * Aggregate operator or must keep a residual.</p>
     *
     * @param aggregation the aggregation request
     * @return present if the connector accepted the aggregation; empty if not supported
     */
    default java.util.Optional<PushdownResult> pushAggregate(
            org.pangrova.spi.connector.metadata.AggregationApplication aggregation) {
        return java.util.Optional.empty();
    }

    /**
     * Asks the connector to absorb a TABLESAMPLE operation.
     *
     * @param percentage sample percentage (0, 100]
     * @param method     sampling method (BERNOULLI or SYSTEM)
     * @return present if the connector accepted the sample; empty if not supported
     */
    default java.util.Optional<PushdownResult> pushSample(
            double percentage, org.pangrova.spi.connector.metadata.SampleInfo.SampleMethod method) {
        return java.util.Optional.empty();
    }
}
