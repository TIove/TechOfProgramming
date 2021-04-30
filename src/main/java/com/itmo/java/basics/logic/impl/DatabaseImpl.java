package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Builder
@AllArgsConstructor
public class DatabaseImpl implements Database {
    private final String name;
    private final Path path;

    private Map<String, Table> tables = new HashMap<>();

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("Name is null");
        }

        Path fullPath;
        try {
            fullPath = Paths.get(databaseRoot.toString() + File.separator + dbName);
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException("IO Exception while creating directory " + dbName + " of data base", e);
        }

        return new DatabaseImpl(dbName, fullPath);
    }

    private DatabaseImpl(String dbName, Path databaseRoot) throws DatabaseException {
        this.name = dbName;
        this.path = databaseRoot;
        this.tables = new HashMap<>();
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        Map<String, Table> tablesMap = context.getTables();
        String name = context.getDbName();
        Path databasePath = context.getDatabasePath();

        return DatabaseImpl.builder()
                .name(name)
                .path(databasePath)
                .tables(tablesMap)
                .build();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null)
            throw new DatabaseException("Table name is null");
        if (tables != null && tables.containsKey(tableName)) {
            throw new DatabaseException("Table with name " + tableName + " already exists");
        }
        try {
            Table table = TableImpl.create(tableName, path, new TableIndex());
            tables.put(tableName, table);
        } catch (DatabaseException e) {
            throw new DatabaseException("Can not create table with name " + tableName, e);
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName))
            throw new DatabaseException("There are not table with name: " + tableName);

        tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName))
            throw new DatabaseException("There are not table with name: " + tableName);
        return tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName))
            throw new DatabaseException("There are not table with name: " + tableName);
        tables.get(tableName).delete(objectKey);
    }
}