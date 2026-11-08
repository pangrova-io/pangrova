package org.pangrova.streaming.maintenance;

import java.time.Duration;

public record RetentionPolicy(
    Duration retentionPeriod,
    String retentionColumn,
    RetentionAction action
) {}
