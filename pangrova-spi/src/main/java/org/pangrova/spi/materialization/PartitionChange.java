package org.pangrova.spi.materialization;

import java.util.List;
import org.pangrova.spi.annotation.Stable;

/**
 * One partition whose rows changed inside a proven change interval.
 *
 * <p>Multi-column partition specs carry one value per partition field, in spec
 * order. The kernel replaces exactly these partitions and copies every other
 * partition from the base generation.
 *
 * @param values identity partition values, in partition spec order
 */
@Stable(since = "1.1")
public record PartitionChange(List<PartitionValue> values) {

    public PartitionChange {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("a partition change must carry at least one value");
        }
        values = List.copyOf(values);
    }
}
