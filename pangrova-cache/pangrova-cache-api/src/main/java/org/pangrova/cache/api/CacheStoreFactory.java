package org.pangrova.cache.api;

import java.util.Map;

/** Installed in the Coordinator. Each successful open transfers one live store to the caller. */
public interface CacheStoreFactory {
    int API_VERSION = 1;
    /** Unique nonblank configuration ID, without surrounding whitespace. disabled is reserved. */
    String id();
    int apiVersion();
    CacheStore open(Map<String, String> settings);
}
