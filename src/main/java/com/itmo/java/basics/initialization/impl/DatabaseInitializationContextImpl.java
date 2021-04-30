package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String name;
    private final Path dbPath;
    private final Map<String, Table> tables = new HashMap<>();

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.name = dbName;
        this.dbPath = Path.of(databaseRoot + File.separator + dbName);
    }

    @Override
    public String getDbName() {
        return this.name;
    }

    @Override
    public Path getDatabasePath() {
        return this.dbPath;
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        String tableName = table.getName();

        if (tables.containsKey(tableName)) {
            throw new RuntimeException("Table" + tableName + "is already exists");
        }

        tables.put(tableName, table);
    }
}
