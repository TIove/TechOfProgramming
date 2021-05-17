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
    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    private final String name;
    private final Path databaseRoot;

    private final Map<String, Table> tables;

    private DatabaseImpl(String dbName, Path databaseRoot) {
        this.name = dbName;
        this.databaseRoot = databaseRoot;
        this.tables = new HashMap<>();
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
            throw new DatabaseException("IO Exception while creating directory " + dbName + " of data base", e);
        }

        return new DatabaseImpl(dbName, fullPath);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        Map<String, Table> tablesMap = context.getTables();

        if (tablesMap == null) {
            tablesMap = new HashMap<>();
        }

        String name = context.getDbName();
        Path databasePath = context.getDatabasePath();

        return DatabaseImpl.builder()
                .name(name)
                .databaseRoot(databasePath)
                .tables(tablesMap)
                .build();
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

        try {
            TableIndex currentTableIndex = new TableIndex();
            Table newTable = TableImpl.create(tableName, databaseRoot, currentTableIndex);

            tables.put(tableName, newTable);
        } catch (DatabaseException e) {
            throw new DatabaseException("Can not create table with name " + tableName, e);
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        var table = tables.get(tableName);

        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        var table = tables.get(tableName);

        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table " + tableName + " doesn't exist");
        }

        var table = tables.get(tableName);

        table.delete(objectKey);
    }
}
