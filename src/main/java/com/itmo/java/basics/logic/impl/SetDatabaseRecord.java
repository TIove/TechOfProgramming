package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] value;
    private final byte[] key;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        final int INT_SIZE = 4;

        return key.length +
                value.length +
                INT_SIZE +
                INT_SIZE;
    }

    @Override
    public boolean isValuePresented() {
        return value != null;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
