package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] _key;

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
        final int INT_SIZE = 4;
        return _key.length + INT_SIZE + INT_SIZE;
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
