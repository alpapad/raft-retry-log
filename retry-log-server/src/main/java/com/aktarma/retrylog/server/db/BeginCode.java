package com.aktarma.retrylog.server.db;

public enum BeginCode {
    OK,
    /**
     * <code>INFLIGHT = 1;</code>
     */
    INFLIGHT,
    /**
     * <code>DUPLICATE = 2;</code>
     */
    DUPLICATE;
}
