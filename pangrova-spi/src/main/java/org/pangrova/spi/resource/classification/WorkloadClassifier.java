package org.pangrova.spi.resource.classification;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI for rule-based query routing and workload classification.
 *
 * <p>A workload classifier maps inbound queries to resource groups based on
 * query attributes (user, source, client tags, query type, database, source IP,
 * and eventually plan cost). The resolved {@link WorkloadClass} drives both
 * admission control and worker placement.</p>
 *
 * <p>This SPI addresses gap G-RES-05: Pangrova had no selector/classifier layer
 * to map inbound queries to resource groups; scheduling used only load-based
 * placement plus an inert fair-share factor.</p>
 *
 * <p>The classifier is the SPI abstraction; the existing
 * {@code ResourceGroupSelector} in {@code pangrova-coordinator} is one
 * implementation (first-match-wins ordered rules, immutable, hot-reload by
 * instance swap). Pluggable implementations can add plan-cost-aware
 * classification, ML-based routing, or external rule stores.</p>
 *
 * <p>Implementations must be thread-safe and should be fast (called on the
 * query submission hot path).</p>
 *
 * @see QueryAttributes
 * @see WorkloadClass
 */
@Experimental(reason = "G-RES-05: workload classification SPI "
        + "-- plan-cost criterion and hot-reload config backend are future work")
public interface WorkloadClassifier {

    /**
     * Classifies a query into a resource group based on its attributes.
     *
     * <p>Implementations must always return a valid classification (never null).
     * When no rule matches, a default resource group should be returned.</p>
     *
     * @param attributes the inbound query attributes
     * @return the workload classification result (never null)
     */
    WorkloadClass classify(QueryAttributes attributes);

    /**
     * Returns the human-readable name of this classifier
     * (e.g. "rule-based", "cost-aware", "ml-router").
     *
     * @return classifier name, never null or blank
     */
    String name();
}
