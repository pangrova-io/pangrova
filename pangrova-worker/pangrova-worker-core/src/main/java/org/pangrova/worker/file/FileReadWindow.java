package org.pangrova.worker.file;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

/** Resolves HTTP single-byte ranges before opening an object stream. */
record FileReadWindow(long offset, long length, boolean partial) {

    private static final Pattern BYTE_RANGE = Pattern.compile("([0-9]*)-([0-9]*)");
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;

    static FileReadWindow resolve(long size, Long offset, Long length, String range) {
        if (range != null) {
            if (offset != null || length != null) {
                throw new BadRequestException("Range cannot be combined with offset or length");
            }
            return fromHeader(size, range.trim());
        }
        long start = offset == null ? 0 : offset;
        long count = length == null || length == -1 ? size - start : length;
        if (start < 0 || start > size || count < 0 || count > size - start
                || (count == 0 && size != 0)) {
            throw unsatisfiable(size);
        }
        return new FileReadWindow(start, count, start != 0 || count != size);
    }

    private static FileReadWindow fromHeader(long size, String header) {
        int equals = header.indexOf('=');
        // Unknown units and multi-range requests are ignored, as permitted by RFC 9110.
        if (equals < 0 || !header.substring(0, equals).equalsIgnoreCase("bytes")
                || header.indexOf(',') >= 0) {
            return new FileReadWindow(0, size, false);
        }
        Matcher match = BYTE_RANGE.matcher(header.substring(equals + 1));
        if (!match.matches() || (match.group(1).isEmpty() && match.group(2).isEmpty())) {
            throw new BadRequestException("Invalid byte range");
        }
        if (size == 0) {
            throw unsatisfiable(size);
        }
        if (match.group(1).isEmpty()) {
            long count = Math.min(decimal(match.group(2)), size);
            if (count == 0) {
                throw unsatisfiable(size);
            }
            return new FileReadWindow(size - count, count, true);
        }
        long start = decimal(match.group(1));
        long end = match.group(2).isEmpty() ? size - 1 : decimal(match.group(2));
        if (start >= size || end < start) {
            throw unsatisfiable(size);
        }
        return new FileReadWindow(start, Math.min(end, size - 1) - start + 1, true);
    }

    private static long decimal(String value) {
        long result = 0;
        for (int index = 0; index < value.length(); index++) {
            int digit = value.charAt(index) - '0';
            if (result > (Long.MAX_VALUE - digit) / 10) {
                return Long.MAX_VALUE;
            }
            result = result * 10 + digit;
        }
        return result;
    }

    private static WebApplicationException unsatisfiable(long size) {
        return new WebApplicationException("Requested range is outside the object",
                Response.status(HTTP_RANGE_NOT_SATISFIABLE).header("Content-Range", "bytes */" + size).build());
    }
}
