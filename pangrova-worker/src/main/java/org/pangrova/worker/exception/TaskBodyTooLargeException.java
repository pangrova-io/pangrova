package org.pangrova.worker.exception;

/** Signals that a Worker task submission crossed its HTTP body limit. */
public final class TaskBodyTooLargeException extends RuntimeException {}
