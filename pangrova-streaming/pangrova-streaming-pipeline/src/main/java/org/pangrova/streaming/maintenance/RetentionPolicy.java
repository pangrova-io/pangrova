package org.pangrova.streaming.maintenance;

import org.pangrova.streaming.enums.RetentionAction;

import java.time.Duration;

public record RetentionPolicy(
    Duration retentionPeriod,
    String retentionColumn,
    RetentionAction action
) {}
