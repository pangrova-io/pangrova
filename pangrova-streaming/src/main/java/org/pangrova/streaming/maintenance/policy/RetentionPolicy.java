package org.pangrova.streaming.maintenance.policy;

import java.time.Duration;

public record RetentionPolicy(
    Duration retentionPeriod,
    String retentionColumn,
    RetentionAction action
) {}
