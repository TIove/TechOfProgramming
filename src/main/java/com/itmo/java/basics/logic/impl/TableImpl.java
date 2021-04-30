package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@Builder
@AllArgsConstructor
public class TableImpl implements Table {
    private final String name;
    private final Path tableRootPath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    private TableImpl(
            String tableName,
            Path tableRootPath,
            TableIndex tableIndex) {
        this.name = tableName;
        this.tableRootPath = tableRootPath;
        this.tableIndex = tableIndex;
    }

    private void validateOrCreateNewSegment() throws DatabaseException {
        if (currentSegment == null || currentSegment.isReadOnly()) {
            var segmentName = SegmentImpl.createSegmentName(name);
            currentSegment = SegmentImpl.create(segmentName, tableRootPath);
        }
    }

    public static Table create(String tableName,
                               Path pathToDatabaseRoot,
                               TableIndex tableIndex) throws DatabaseException {
        Path fullPath = Paths.get(pathToDatabaseRoot.toString() + File.separator + tableName);
        try {
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException("Exception while creating stream for path - " + fullPath, e);
        }

        Table table = new TableImpl(tableName, fullPath, tableIndex);

        return CachingTable.builder()
                .table(table)
                .build();
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        TableIndex tableIndex = context.getTableIndex();
        Segment currentSegment = context.getCurrentSegment();
        String tableName = context.getTableName();
        Path tablePath = context.getTablePath();

        Table table = new TableImpl(tableName, tablePath, tableIndex, currentSegment);

        return CachingTable.builder()
                .table(table)
                .build();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            validateOrCreateNewSegment();

            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
            currentSegment.write(objectKey, objectValue);

        } catch (IOException exc) {
            throw new DatabaseException("Exception while writing new data", exc);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        try {
            if (segment.isPresent()) {
                return segment.get().read(objectKey);
            } else {
                return Optional.empty();
            }
        } catch (IOException exc) {
            throw new DatabaseException("Exception while reading from segment - " + segment.get().getName(), exc);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (tableIndex.searchForKey(objectKey).isEmpty()) {
            throw new DatabaseException("Key - " + objectKey + " wasn't used");
        }

        validateOrCreateNewSegment();

        try {
            currentSegment.delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException exc) {
            throw new DatabaseException("Exception while writing RemoveDbRecord in - " + currentSegment, exc);
        }
    }
}
