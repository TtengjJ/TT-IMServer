package com.crossoverjie.cim.client.sdk.io;

import java.util.LinkedHashMap;
import java.util.Map;

public final class MessageIdDeduper {

    private static final int DEFAULT_CAPACITY = 4096;
    private final int capacity;
    private final Map<Long, Boolean> seen;

    public MessageIdDeduper() {
        this(DEFAULT_CAPACITY);
    }

    public MessageIdDeduper(int capacity) {
        this.capacity = capacity;
        this.seen = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                return size() > MessageIdDeduper.this.capacity;
            }
        };
    }

    public synchronized boolean checkAndRemember(Long messageId) {
        if (messageId == null) {
            return false;
        }
        if (seen.containsKey(messageId)) {
            return true;
        }
        seen.put(messageId, Boolean.TRUE);
        return false;
    }
}
