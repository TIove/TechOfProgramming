package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.executionEnvironment().getWorkingPath();

        File[] dbDirectories = new File(workingPath.toString())
                .listFiles(File::isDirectory);

        if (dbDirectories == null) {
            throw new DatabaseException("There is no any DataBases on path - " + workingPath);
        }

        Arrays.sort(dbDirectories);

        for (File currentDb : dbDirectories) {
            var dataBaseContext = new DatabaseInitializationContextImpl(
                    currentDb.getName(),
                    workingPath);

            InitializationContext currentContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(dataBaseContext)
                    .build();

            databaseInitializer.perform(currentContext);
        }
    }
}
