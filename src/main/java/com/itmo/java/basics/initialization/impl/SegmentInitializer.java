package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.impl.TableImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;


public class SegmentInitializer implements Initializer {
    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path segmentFullPath = context.currentSegmentContext().getSegmentPath();
        Path tableFullPath = context.currentTableContext().getTablePath();

        String segmentName = context.currentSegmentContext().getSegmentName();
        String tableName = context.currentTableContext().getTableName();

        SegmentIndex segmentIndex = new SegmentIndex();
        long currentOffset = 0;

        try (FileInputStream fileInputStream = new FileInputStream(segmentFullPath.toString());
             DataInputStream dataInputStream = new DataInputStream(fileInputStream);
             DatabaseInputStream inputStream = new DatabaseInputStream(dataInputStream)) {

            while (currentOffset < Files.size(segmentFullPath)) {
                var dbRecord = inputStream.readDbUnit();

                if (dbRecord.isPresent() && dbRecord.get().getValue() != null) {
                    String key = new String(dbRecord.get().getKey(), StandardCharsets.UTF_8);
                    byte[] value = dbRecord.get().getValue();

                    SegmentOffsetInfo segmentOffsetInfo = new SegmentOffsetInfoImpl(currentOffset);
                    segmentIndex.onIndexedEntityUpdated(key, segmentOffsetInfo);

                    var currentSegmentContext = new SegmentInitializationContextImpl(
                            segmentName,
                            segmentFullPath,
                            (int) currentOffset,
                            segmentIndex);

                    var newSegment = SegmentImpl.initializeFromContext(currentSegmentContext);

                    context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, newSegment);

                    currentOffset += 4 + key.getBytes(StandardCharsets.UTF_8).length + 4 + value.length;
                }
            }

            var currentSegmentContext = new SegmentInitializationContextImpl(
                    segmentName,
                    tableFullPath,
                    (int) currentOffset,
                    segmentIndex);

            var newSegment = SegmentImpl.initializeFromContext(currentSegmentContext);
            context.currentTableContext().updateCurrentSegment(newSegment);

        } catch (IOException exc) {
            throw new DatabaseException("IOException while reading segment " + segmentName, exc);
        }
    }
}
