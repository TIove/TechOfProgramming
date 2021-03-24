package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private byte[] _key;

    public RemoveDatabaseRecord(byte[] key) {
        _key = key;
    }

    @Override
    public byte[] getKey() {
        return _key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        int keyUTFSize = String.valueOf(_key.length).getBytes(StandardCharsets.UTF_8).length;
        return _key.length + keyUTFSize + 2;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return _key.length;
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}
