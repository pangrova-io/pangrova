package org.pangrova.worker.ai.function;

import java.util.Map;

/** Determines whether AI models or governed FILE storage are explicitly configured. */
public final class AiFunctionActivation {

    private AiFunctionActivation() {}

    /** Returns whether at least one exact AI model descriptor is configured. */
    public static boolean hasModels(Map<String, String> config) {
        return config != null && config.keySet().stream()
                .anyMatch(key -> key.startsWith("models."));
    }

    /** Returns whether governed FILE storage was explicitly enabled. */
    public static boolean hasFileStorage(Map<String, String> config) {
        if (config == null) {
            return false;
        }
        String backend = config.get("file.storage-backend");
        return backend != null && !backend.isBlank();
    }
}
