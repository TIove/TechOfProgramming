package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String _name;
    private final Path _pathToDatabaseRoot;
    private final HashMap<String, Table> _tables = new HashMap<>();
    private final HashMap<String, TableIndex> _tableIndexMap = new HashMap<>();

    private DatabaseImpl(String dbName, Path pathToDatabaseRoot) {
        _name = dbName;
        _pathToDatabaseRoot = pathToDatabaseRoot;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("Name is null");
        }

        Path fullPath;
        try {
            fullPath = Paths.get(databaseRoot.toString() + "/" + dbName);
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return new DatabaseImpl(dbName, fullPath);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (_tables.containsKey(tableName))
            throw new DatabaseException("This table name already exists");
        if (tableName == null)
            throw new DatabaseException("Name is null");
        TableIndex currentTableIndex = new TableIndex();

        _tableIndexMap.put(tableName, currentTableIndex);
        _tables.put(tableName, TableImpl.create(tableName, _pathToDatabaseRoot, currentTableIndex));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        var table = _tables.get(tableName);

        if (table == null) {
            throw new DatabaseException("Table doesn't exist");
        }

        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        var table = _tables.get(tableName);

        if (table == null) {
            throw new DatabaseException("Table doesn't exist");
        }

        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        var table = _tables.get(tableName);

        table.delete(objectKey);
    }
}
