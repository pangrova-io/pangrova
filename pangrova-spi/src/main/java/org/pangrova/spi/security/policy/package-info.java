/**
 * Security-owned privilege, role and grant contracts and their lexical validation.
 * These public contracts are consumed by policy providers and engine adapters;
 * they must not depend on security core implementations, persistence or SQL planners.
 * SQL parsing and authorization remain separate from lexical validation.
 */
package org.pangrova.spi.security.policy;
