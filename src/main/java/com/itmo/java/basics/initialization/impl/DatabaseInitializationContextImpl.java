package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String name;
    private final Path rootPath;
    private final Map<String, Table> tables = new HashMap<>();

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.name = dbName;
        this.rootPath = databaseRoot;
    }

    @Override
    public String getDbName() {
        return name;
    }

    @Override
    public Path getDatabasePath() {
        return rootPath;
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        String tableName = table.getName();

        if (tables.containsKey(tableName))
        {
            throw new RuntimeException("Table" + tableName + "is already exists");
        }

        tables.put(tableName, table);
    }
}
