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

        if (value == null || key == null) {
            return INT_SIZE + 8 + INT_SIZE + 8;
        }
        return INT_SIZE + key.length + INT_SIZE + value.length;
    }

    @Override
    public boolean isValuePresented() {
        return value != null && value.length != 0;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        if (value == null || value.length == 0) {
            return -1;
        }

        return value.length;
    }
}
