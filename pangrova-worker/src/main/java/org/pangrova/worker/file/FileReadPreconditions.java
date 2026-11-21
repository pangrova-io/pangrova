package org.pangrova.worker.file;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.ws.rs.BadRequestException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.object.FileStat;

/** Evaluates strong HTTP entity tags independently from provider version identifiers. */
final class FileReadPreconditions {
    private static final Pattern ENTITY_TAG = Pattern.compile("(?:W/)?\"[\\x21\\x23-\\x7E\\x80-\\xFF]*\"");

    private FileReadPreconditions() {
    }

    static String entityTag(FileStat stat) {
        String tag = stat.etag();
        if (tag == null) {
            return null;
        }
        return tag.startsWith("\"") || tag.startsWith("W/\"") ? tag : '"' + tag + '"';
    }

    static void check(FileStat stat, String version, String ifMatch) {
        if (version != null && !version.equals(stat.providerVersion())) {
            throw failed();
        }
        if (ifMatch == null || ifMatch.trim().equals("*")) {
            return;
        }
        String header = ifMatch.trim();
        String current = entityTag(stat);
        Matcher matcher = ENTITY_TAG.matcher(header);
        boolean matched = false;
        int cursor = 0;
        do {
            matcher.region(cursor, header.length());
            if (!matcher.lookingAt()) {
                throw new BadRequestException("Invalid If-Match entity tag list");
            }
            String candidate = matcher.group();
            matched |= !candidate.startsWith("W/") && candidate.equals(current);
            cursor = matcher.end();
            while (cursor < header.length() && isWhitespace(header.charAt(cursor))) {
                cursor++;
            }
            if (cursor == header.length()) {
                break;
            }
            if (header.charAt(cursor++) != ',') {
                throw new BadRequestException("Invalid If-Match entity tag list");
            }
            while (cursor < header.length() && isWhitespace(header.charAt(cursor))) {
                cursor++;
            }
            if (cursor == header.length()) {
                throw new BadRequestException("Invalid If-Match entity tag list");
            }
        } while (cursor < header.length());
        if (!matched) {
            throw failed();
        }
    }

    private static boolean isWhitespace(char value) {
        return value == ' ' || value == '\t';
    }

    private static PangrovaException failed() {
        return new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED,
                "File read precondition failed");
    }
}
