package org.pangrova.security.core.authz;

/**
 * Access control interface for imperative privilege checks and declarative policy retrieval.
 * Extends the SPI AccessControl contract — implementations in this module provide RBAC semantics.
 */
public interface DefaultAccessControl extends org.pangrova.security.api.AccessControl {
}
