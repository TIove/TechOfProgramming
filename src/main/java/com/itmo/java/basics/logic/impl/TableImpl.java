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
    private Path path;
    private final TableIndex index;
    private Segment lastSegment = null;

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        try {
            Table table = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
            return table;
        } catch (IOException e) {
            throw new DatabaseException("Table creation error", e);
        }
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws IOException {
        name = tableName;
        path = pathToDatabaseRoot;
        index = tableIndex;
        Path beforeCreationPath = Paths.get(pathToDatabaseRoot.toString() + File.separator + tableName);
        Files.createDirectory(beforeCreationPath);
        path = beforeCreationPath;
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

    public void makeLastSegmentActual() throws DatabaseException {
        if (lastSegment == null || lastSegment.isReadOnly()) {
            lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
        }
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        makeLastSegmentActual();
        try {
            lastSegment.write(objectKey, objectValue);
            index.onIndexedEntityUpdated(objectKey, lastSegment);

        } catch (IOException e) {
            throw new DatabaseException("Table " + name + " write error: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = index.searchForKey(objectKey);
        if (segment.isEmpty())
            return Optional.empty();
        try {
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not read from segment: " + segment.get().getName() + " with error: "
                    + e.getMessage());
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        makeLastSegmentActual();
        try {
            lastSegment.delete(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not delete from segment: " + lastSegment.getName() + " with error: "
                    + e.getMessage());
        }
    }
}
