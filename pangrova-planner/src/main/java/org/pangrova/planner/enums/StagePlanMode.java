package org.pangrova.planner.enums;

/** Replay is immutable: no adaptive reoptimization or fresh connector split enumeration. */
public enum StagePlanMode { LIVE_QUERY, DURABLE_REPLAY }
