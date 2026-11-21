package org.pangrova.worker.exception;

/** Exact control-plane rejection, distinct from a failed or unknown transport exchange. */
public final class CopySourceReadRejectedException extends IllegalStateException {
    private final int status;

    public CopySourceReadRejectedException(String action, int status) {
        super("COPY source-read " + action + " rejected with status " + status);
        this.status = status;
    }
    public int status() { return status; }
    public boolean isPolicyDenial() {
        return status == java.net.HttpURLConnection.HTTP_UNAUTHORIZED
                || status == java.net.HttpURLConnection.HTTP_FORBIDDEN;
    }
}
