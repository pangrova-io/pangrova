package org.pangrova.coordinator.http;

/**
 * Utility for extracting queryId from HTTP request path segments.
 *
 * <p>Shared by any resource that needs to parse queryId
 * from a URI path like {@code /v1/query/{queryId}/state}.</p>
 */
public final class QueryPathUtils {

    private QueryPathUtils() {}

    /**
     * Extracts the queryId from a path by finding the segment immediately
     * before the given action segment.
     *
     * <p>Example: {@code extractQueryId("/v1/query/abc-123/state", "state")}
     * returns {@code "abc-123"}.
     *
     * @param pathInfo      the request path (e.g. {@code /v1/query/abc-123/cancel})
     * @param actionSegment the action segment that follows the queryId (e.g. "state", "cancel")
     * @return the queryId segment, or {@code null} if not found
     * @throws IllegalArgumentException if pathInfo is null or blank
     */
    public static String extractQueryId(String pathInfo, String actionSegment) {
        if (pathInfo == null || pathInfo.isBlank()
                || actionSegment == null || actionSegment.isBlank()) {
            throw new IllegalArgumentException("pathInfo and actionSegment must not be null or blank");
        }

        String[] segments = pathInfo.split("/");
        for (int i = 1; i < segments.length; i++) {
            if (segments[i].equals(actionSegment) && !segments[i - 1].isBlank()) {
                return segments[i - 1];
            }
        }
        return null;
    }
}
