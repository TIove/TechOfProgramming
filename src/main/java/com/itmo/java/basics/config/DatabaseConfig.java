package com.itmo.java.basics.config;

import java.io.File;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private final String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath == null ? DEFAULT_WORKING_PATH : workingPath;

        var dir = new File(this.workingPath);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    public String getWorkingPath() {
        if (workingPath == null) {
            return DEFAULT_WORKING_PATH;
        }

        return workingPath;
    }
}
