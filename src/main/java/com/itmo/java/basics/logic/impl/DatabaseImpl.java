package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String name;
    private final Path pathToDatabaseRoot;
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, TableIndex> tableIndexMap = new HashMap<>();

    private DatabaseImpl(String dbName, Path pathToDatabaseRoot) {
        this.name = dbName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("Name is null");
        }

        Path fullPath;
        try {
            fullPath = Paths.get(databaseRoot.toString() + File.separator + dbName);
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException("IO Exception while creating directory of data base", e);
        }

        return new DatabaseImpl(dbName, fullPath);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name is null");
        }

        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table name - " + tableName + " already exists");
        }

        TableIndex currentTableIndex = new TableIndex();

        tableIndexMap.put(tableName, currentTableIndex);
        tables.put(tableName, TableImpl.create(tableName, pathToDatabaseRoot, currentTableIndex));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        var table = tables.get(tableName);

        if (table == null) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        var table = tables.get(tableName);

        if (table == null) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        var table = tables.get(tableName);

        if (table == null) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        table.delete(objectKey);
    }
}
