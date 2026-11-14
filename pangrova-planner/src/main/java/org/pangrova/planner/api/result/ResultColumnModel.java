package org.pangrova.planner.api.result;

import org.pangrova.common.api.annotation.Internal;

/** Exact SQL-visible model origin for one result column; resolved to model identity at dispatch. */
@Internal
public record ResultColumnModel(int columnIndex, String modelName) {}
