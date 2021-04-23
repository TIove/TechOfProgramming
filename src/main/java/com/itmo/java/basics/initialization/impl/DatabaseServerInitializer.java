package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

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

        for (File currentDb : dbDirectories) {
            var dataBaseContext = new DatabaseInitializationContextImpl(currentDb.getName(), currentDb.toPath());

            InitializationContext currentContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(dataBaseContext)
                    .build();

            databaseInitializer.perform(currentContext);
        }

        Database newDatabase = DatabaseImpl.initializeFromContext(context.currentDbContext());
        context.executionEnvironment().addDatabase(newDatabase);
    }
}
