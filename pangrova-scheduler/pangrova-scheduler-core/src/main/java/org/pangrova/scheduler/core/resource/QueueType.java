package org.pangrova.scheduler.core.resource;

/**
 * Resource queue types with scheduling weights.
 */
public enum QueueType {

    INTERACTIVE(3.0),
    BATCH(1.0),
    BACKGROUND(0.5);

    private final double weight;

    QueueType(double weight) {
        this.weight = weight;
    }

    public double getWeight() { return weight; }
}
