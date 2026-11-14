package org.pangrova.connector.api.metadata;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;

/** SDK manifest preparation header; its bounded new-object pages must all be sealed before catalog acceptance. */
@Experimental
public record PreparedSourcePublication(AcceptedSourceSnapshot candidate, long rows, long objectsCount,
        byte[] objectsSha256, String inheritedMembership) {
    public PreparedSourcePublication {
        java.util.Objects.requireNonNull(candidate);
        objectsSha256 = objectsSha256.clone();
        if (rows < 0 || objectsCount <= 0
                || objectsCount > (long) SourceObjectPage.MAX_PAGES * SourceObjectPage.MAX_PAGE_OBJECTS
                || objectsSha256.length != 32 || inheritedMembership == null
                || (!inheritedMembership.isEmpty() && !DigestUtils.isSha256Hex(inheritedMembership))) {
            throw new IllegalArgumentException("Invalid prepared source object header");
        }
    }
    @Override public byte[] objectsSha256() { return objectsSha256.clone(); }
}
