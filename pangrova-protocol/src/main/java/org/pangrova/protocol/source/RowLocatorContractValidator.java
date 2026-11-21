package org.pangrova.protocol.source;

import org.pangrova.protocol.v2.RowLocatorContract;
import org.pangrova.protocol.v2.RowLocatorKind;
import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;

/** Shared fail-closed validation for the canonical generic row-locator contract. */
@Internal
public final class RowLocatorContractValidator {

    private RowLocatorContractValidator() {}

    /**
     * Validates and returns a supported canonical row-locator contract.
     *
     * @param contract contract to validate
     * @return the validated contract
     * @throws IllegalArgumentException if the contract is incomplete or unsupported
     */
    public static RowLocatorContract validate(RowLocatorContract contract) {
        Objects.requireNonNull(contract, "row locator contract must not be null");
        validateSchemaIdentity(contract);
        if (contract.getKind() == RowLocatorKind.ROW_LOCATOR_KIND_PHYSICAL_ROW_POSITION) {
            return validatePhysicalPosition(contract);
        }
        if (contract.getKind() == RowLocatorKind.ROW_LOCATOR_KIND_PRIMARY_KEY) {
            return validatePrimaryKey(contract);
        }
        throw new IllegalArgumentException("row locator kind must be explicitly supported");
    }

    private static void validateSchemaIdentity(RowLocatorContract contract) {
        if (contract.getSchemaVersion() == 0 || contract.getSchemaSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "row locator schema version and SHA-256 must be explicit");
        }
    }

    private static RowLocatorContract validatePhysicalPosition(RowLocatorContract contract) {
        if (!contract.getSourceObjectScoped() || contract.getKeyTypesCount() != 0) {
            throw new IllegalArgumentException(
                    "physical row positions must be source-object scoped and have no key types");
        }
        return contract;
    }

    private static RowLocatorContract validatePrimaryKey(RowLocatorContract contract) {
        if (contract.getSourceObjectScoped() || contract.getKeyTypesCount() == 0
                || contract.getKeyTypesList().stream().anyMatch(type ->
                type.getCanonicalSql().isBlank() || !type.hasSubstraitType())) {
            throw new IllegalArgumentException(
                    "primary-key row locators require ordered typed keys and cannot be source-object scoped");
        }
        return contract;
    }
}
