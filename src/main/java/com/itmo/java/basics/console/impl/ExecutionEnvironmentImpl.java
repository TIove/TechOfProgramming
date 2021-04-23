package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final Path workingPath;
    private final Map<String, Database> databaseMap = new HashMap<>();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.workingPath = Path.of(config.getWorkingPath());
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.of(databaseMap.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        databaseMap.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return workingPath;
    }
}
