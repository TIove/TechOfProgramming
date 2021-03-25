package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {
    private final String _name;
    private final Path _tableRootPath;
    private final TableIndex _tableIndex;
    private Segment _currentSegment;

    private TableImpl(
            String tableName,
            Path tableRootPath,
            TableIndex tableIndex) {
        _name = tableName;
        _tableRootPath = tableRootPath;
        _tableIndex = tableIndex;
    }

    private void validateOrCreateNewSegment() throws DatabaseException {
        if (_currentSegment == null || _currentSegment.isReadOnly()) {
            var segmentName = SegmentImpl.createSegmentName(_name);
            _currentSegment = SegmentImpl.create(segmentName, _tableRootPath);
        }
    }

    static Table create(String tableName,
                        Path pathToDatabaseRoot,
                        TableIndex tableIndex) throws DatabaseException {
        Path fullPath = Paths.get(pathToDatabaseRoot.toString() + '/' + tableName);
        try {
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException("Exception while creating stream for path - " + fullPath.toString(), e);
        }

        return new TableImpl(tableName, fullPath, tableIndex);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            validateOrCreateNewSegment();

            _tableIndex.onIndexedEntityUpdated(objectKey, _currentSegment);
            _currentSegment.write(objectKey, objectValue);

        } catch (IOException exc) {
            throw new DatabaseException("Exception while writing new data", exc);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = _tableIndex.searchForKey(objectKey);
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
        if (_tableIndex.searchForKey(objectKey).isEmpty()) {
            throw new DatabaseException("Key - " + objectKey + " wasn't used");
        }

        validateOrCreateNewSegment();

        try {
            _currentSegment.delete(objectKey);
            _tableIndex.onIndexedEntityUpdated(objectKey, _currentSegment);
        } catch (IOException exc) {
            throw new DatabaseException("Exception while writing RemoveDbRecord in - " + _currentSegment, exc);
        }
    }
}
