package org.pangrova.execution.api.enums;

/** Semantic task purpose before canonical wire binding; unsupported purposes are rejected at the caller. */
public enum TaskPurpose {
    COMPUTE, OUTPUT, WRITE, BACKGROUND_BUILD, BACKGROUND_VALIDATE, BACKGROUND_EXPORT
}
