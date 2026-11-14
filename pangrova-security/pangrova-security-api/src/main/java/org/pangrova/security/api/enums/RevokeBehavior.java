package org.pangrova.security.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Handling of dependent grants during revocation. */
@Experimental
public enum RevokeBehavior {
    RESTRICT,
    CASCADE
}
