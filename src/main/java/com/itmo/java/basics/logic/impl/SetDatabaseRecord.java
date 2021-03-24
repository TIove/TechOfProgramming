package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private byte[] _value;
    private byte[] _key;
    private int _size;

    public SetDatabaseRecord(byte[] value, byte[] key) {
        _key = key;
        _value = value;

        _size = key.length + value.length;
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
        int keyUTFSize = String.valueOf(_key.length).getBytes(StandardCharsets.UTF_8).length;
        int valueUTFSize = String.valueOf(_value.length).getBytes(StandardCharsets.UTF_8).length;

        return _key.length +
                _value.length +
                valueUTFSize +
                keyUTFSize;
    }

    @Override
    public boolean isValuePresented() {
        return _value.length > 0;
    }

    @Override
    public int getKeySize() {
        return _key.length;
    }

    @Override
    public int getValueSize() {
        return _value.length == 0 ? -1 : _value.length;
    }
}
