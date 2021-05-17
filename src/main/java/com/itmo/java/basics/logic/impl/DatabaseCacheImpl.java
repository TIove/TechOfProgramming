package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;

    private final int capacity;
    private final HashMap<String, byte[]> map;

    public DatabaseCacheImpl(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<>(capacity, 1, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return capacity <= size();
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return map.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        delete(key);

        this.map.put(key, value);
    }

    @Override
    public void delete(String key) {

    }
}
