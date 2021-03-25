package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] _value;
    private final byte[] _key;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        _key = key;
        _value = value;
    }

    @Override
    public byte[] getKey() {
        return _key;
    }

    @Override
    public byte[] getValue() {
        return _value;
    }

    @Override
    public long size() {
        final int INT_SIZE = 4;

        return _key.length +
                _value.length +
                INT_SIZE +
                INT_SIZE;
    }

    @Override
    public boolean isValuePresented() {
        return _value != null;
    }

    @Override
    public int getKeySize() {
        return _key.length;
    }

    @Override
    public int getValueSize() {
        return _value.length;
    }
}
