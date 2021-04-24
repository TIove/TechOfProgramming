package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path databasePath = context.currentDbContext().getDatabasePath();

        File[] tableDirectories = new File(databasePath.toString())
                .listFiles(File::isDirectory);

        if (tableDirectories == null) {
            throw new DatabaseException("There is no any Tables on path - " + databasePath);
        }

        for (File currentTable : tableDirectories) {
            var tableIndex = new TableIndex();
            var tableContext = new TableInitializationContextImpl(
                    currentTable.getName(),
                    Path.of(currentTable.getAbsolutePath()),
                    tableIndex);

            InitializationContext currentContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(tableContext)
                    .build();

            tableInitializer.perform(currentContext);
        }

        Database newDatabase = DatabaseImpl.initializeFromContext(context.currentDbContext());
        context.executionEnvironment().addDatabase(newDatabase);
    }
}
