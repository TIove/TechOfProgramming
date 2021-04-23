package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path tablePath = context.currentTableContext().getTablePath();

        File[] segmentFiles = new File(tablePath.toString())
                .listFiles(File::isFile);

        if (segmentFiles == null) {
            throw new DatabaseException("There is no any Segments on path - " + tablePath);
        }

        for (File currentSegment : segmentFiles) {
            var segmentContext = new SegmentInitializationContextImpl(
                    currentSegment.getName(),
                    context.currentTableContext().getTablePath(),
                    SegmentInitializationContextImpl.DEFAULT_INDEX_SIZE
                    );

            InitializationContext currentContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(segmentContext)
                    .build();

            segmentInitializer.perform(currentContext);
        }

        var newTable = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(newTable);
    }
}
