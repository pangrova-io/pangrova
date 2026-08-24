package org.pangrova.planner.cost;

/**
 * Network cost weights based on data locality topology.
 * local=0, rackLocal=10, crossRack=100.
 */
public final class TopologyAwareNetworkCost {

    public static final double LOCAL_COST = 0.0;
    public static final double RACK_LOCAL_COST = 10.0;
    public static final double CROSS_RACK_COST = 100.0;

    private TopologyAwareNetworkCost() {}

    /**
     * Return the network transfer cost for the given locality.
     */
    public static double costFor(Locality locality) {
        return switch (locality) {
            case NODE_LOCAL -> LOCAL_COST;
            case RACK_LOCAL -> RACK_LOCAL_COST;
            case REMOTE -> CROSS_RACK_COST;
        };
    }
}
