package org.pangrova.file.api.enums;

import java.util.Locale;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Operation bound by a file grant; wire names use lower-case ASCII. */
@org.pangrova.common.api.annotation.Experimental
public enum FileOperation {
    HEAD, LIST, READ, WRITE, DELETE;

    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static FileOperation parse(String wireName) {
        return switch (wireName == null ? "" : wireName.trim().toLowerCase(Locale.ROOT)) {
            case "head" -> HEAD;
            case "list" -> LIST;
            case "read" -> READ;
            case "write" -> WRITE;
            case "delete" -> DELETE;
            default -> throw new PangrovaException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "File operation must be one of head, list, read, write, delete");
        };
    }
}
