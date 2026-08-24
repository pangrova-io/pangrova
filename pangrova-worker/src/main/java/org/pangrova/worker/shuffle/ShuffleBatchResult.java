package org.pangrova.worker.shuffle;

/**
 * Discriminated result from native shuffle batch retrieval.
 * Distinguishes DATA (valid batch), EOF (no more data), and ERROR (native failure).
 */
public record ShuffleBatchResult(Status status, byte[] data, String errorMessage) {

    public enum Status { DATA, EOF, ERROR }

    public static ShuffleBatchResult data(byte[] payload) {
        return new ShuffleBatchResult(Status.DATA, payload, null);
    }

    public static ShuffleBatchResult eof() {
        return new ShuffleBatchResult(Status.EOF, null, null);
    }

    public static ShuffleBatchResult error(String message) {
        return new ShuffleBatchResult(Status.ERROR, null, message);
    }
}
