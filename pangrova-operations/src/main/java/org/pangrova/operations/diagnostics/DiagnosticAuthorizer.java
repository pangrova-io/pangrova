package org.pangrova.operations.diagnostics;

/**
 * Strategy interface for authorizing diagnostic operations.
 * Implementations should throw {@link DiagnosticException} if the caller
 * is not permitted to execute the requested operation.
 *
 * <p>The default implementation ({@link #ALLOW_ALL}) permits all operations
 * and is intended only for testing or non-production deployments.
 */
@FunctionalInterface
public interface DiagnosticAuthorizer {

    /** Required role for heap dump operations in the default role-based authorizer. */
    String ADMIN_ROLE = "admin";

    /** Required role for heap dump operations (alternative). */
    String OPERATOR_ROLE = "operator";

    /**
     * Checks whether the given caller is authorized to perform a heap dump.
     *
     * @param caller the caller context
     * @throws DiagnosticException if the caller lacks authorization
     */
    void checkHeapDumpPermission(CallerContext caller) throws DiagnosticException;

    /**
     * An authorizer that permits all operations without any checks.
     * Use only for testing or explicitly unsecured deployments.
     */
    DiagnosticAuthorizer ALLOW_ALL = caller -> {};

    /**
     * Creates a role-based authorizer that requires the caller to have
     * either the {@code admin} or {@code operator} role.
     */
    static DiagnosticAuthorizer requireAdminOrOperator() {
        return caller -> {
            if (caller == null) {
                throw new DiagnosticException("Authentication required for heap dump");
            }
            if (!caller.hasRole(ADMIN_ROLE) && !caller.hasRole(OPERATOR_ROLE)) {
                throw new DiagnosticException(
                        "Access denied: heap dump requires 'admin' or 'operator' role, "
                                + "caller '" + caller.principal() + "' has roles " + caller.roles());
            }
        };
    }
}
