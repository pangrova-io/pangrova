package org.pangrova.streaming.core.maintenance;

import java.time.Duration;

public record RetentionPolicy(
    Duration retentionPeriod,
    String retentionColumn,
    RetentionAction action
) {}
