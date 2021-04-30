package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class DatabaseCacheImpl implements DatabaseCache {
    private final int capacity;
    private final LinkedHashMap<String, byte[]> map;

    public DatabaseCacheImpl(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<>(capacity, 1, true);
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
