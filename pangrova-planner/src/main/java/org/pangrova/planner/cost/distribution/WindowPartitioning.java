package org.pangrova.planner.cost.distribution;

import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.RelNode;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;
import java.util.function.BiFunction;

/** Distribution satisfying every window group over one semantically complete input stream. */
@Internal
public final class WindowPartitioning {
    private WindowPartitioning() {}

    /** Uses only keys shared by every group; no shared key requires the complete stream on one worker. */
    public static RelNode enforce(Window window, BiFunction<RelNode, PangrovaDistribution, RelNode> inputDistribution) {
        if (window.groups.isEmpty()) {
            throw new IllegalArgumentException("Window stage requires at least one window group");
        }
        var commonKeys = window.groups.get(0).keys;
        for (var group : window.groups) {
            commonKeys = commonKeys.intersect(group.keys);
        }
        var distribution = commonKeys.isEmpty()
                ? PangrovaDistribution.SINGLETON : PangrovaDistribution.hash(commonKeys.asList());
        var input = inputDistribution.apply(window.getInput(), distribution);
        return input == window.getInput() ? window : window.copy(window.getTraitSet(), List.of(input));
    }
}
