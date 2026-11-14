package org.pangrova.cache.api;

import java.util.Map;

import org.pangrova.common.api.annotation.Stable;

/** Installed in the Coordinator. Each successful open transfers one live store to the caller. */
@Stable(since = "1.0")
public interface CacheStoreFactory {
    int API_VERSION = 1;
    /** Unique nonblank configuration ID, without surrounding whitespace. disabled is reserved. */
    String id();
    int apiVersion();
    CacheStore open(Map<String, String> settings);
}
