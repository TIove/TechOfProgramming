package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.Optional;

@Builder
@AllArgsConstructor
public class CachingTable implements Table {
    private final DatabaseCacheImpl databaseCache = new DatabaseCacheImpl(5000);
    private final Table table;

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        databaseCache.set(objectKey, objectValue);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] value = databaseCache.get(objectKey);

        if (value == null) {
            return table.read(objectKey);
        } else {
            return Optional.of(value);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        databaseCache.delete(objectKey);
        table.delete(objectKey);
    }
}
