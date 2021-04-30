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
        if(context == null ||
                context.currentSegmentContext() == null ||
                context.currentTableContext() == null ||
                context.currentSegmentContext() == null) {

            throw new DatabaseException("Bad info in context");
        }

        Path segmentFullPath = context.currentSegmentContext().getSegmentPath();
        Path tableFullPath = context.currentTableContext().getTablePath();
        String segmentName = context.currentSegmentContext().getSegmentName();

        SegmentIndex segmentIndex = new SegmentIndex();
        long currentOffset = 0;

        try (FileInputStream fileInputStream = new FileInputStream(segmentFullPath.toString());
             DataInputStream dataInputStream = new DataInputStream(fileInputStream);
             DatabaseInputStream inputStream = new DatabaseInputStream(dataInputStream)) {

            long segmentSize = Files.size(segmentFullPath);
            while (currentOffset < segmentSize) {
                var dbRecord = inputStream.readDbUnit();

                if (dbRecord.isPresent()) {
                    String key = new String(dbRecord.get().getKey(), StandardCharsets.UTF_8);
                    byte[] value = dbRecord.get().getValue();

                    int keySize = key.getBytes(StandardCharsets.UTF_8).length;
                    int valueSize = 0;

                    if (value != null) {
                        valueSize = value.length;
                    }

                    SegmentOffsetInfo segmentOffsetInfo = new SegmentOffsetInfoImpl(currentOffset);
                    segmentIndex.onIndexedEntityUpdated(key, segmentOffsetInfo);

                    var currentSegmentContext = new SegmentInitializationContextImpl(
                            segmentName,
                            segmentFullPath,
                            (int) segmentSize,
                            segmentIndex);

                    var newSegment = SegmentImpl.initializeFromContext(currentSegmentContext);

                    context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, newSegment);

                    currentOffset += 4 + keySize + 4 + valueSize;
                } else {
                    throw new DatabaseException("Bad segment " + segmentName);
                }
            }

            var currentSegmentContext = new SegmentInitializationContextImpl(
                    segmentName,
                    segmentFullPath,
                    (int) currentOffset,
                    segmentIndex);

            var newSegment = SegmentImpl.initializeFromContext(currentSegmentContext);
            context.currentTableContext().updateCurrentSegment(newSegment);

        } catch (IOException exc) {
            throw new DatabaseException("IOException while reading segment " + segmentName, exc);
        }
    }
}
