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

    static Table create(String tableName,
                        Path pathToDatabaseRoot,
                        TableIndex tableIndex) throws DatabaseException {
        Path fullPath;
        try {
            fullPath = Paths.get(pathToDatabaseRoot.toString() + '/' + tableName);
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException(e);
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
            if (_currentSegment == null || _currentSegment.isReadOnly()) {
                var segmentName = SegmentImpl.createSegmentName(_name);
                _currentSegment = SegmentImpl.create(segmentName, _tableRootPath);
            }

            _tableIndex.onIndexedEntityUpdated(objectKey, _currentSegment);
            _currentSegment.write(objectKey, objectValue);

        } catch (IOException exc) {
            throw new DatabaseException(exc);
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
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (_tableIndex.searchForKey(objectKey).isEmpty()) {
            throw new DatabaseException("This key wasn't used");
        }

        if (_currentSegment == null || _currentSegment.isReadOnly()) {
            var segmentName = SegmentImpl.createSegmentName(_name);
            _currentSegment = SegmentImpl.create(segmentName, _tableRootPath);
        }

        try {
            _tableIndex.onIndexedEntityUpdated(objectKey, _currentSegment);
            _currentSegment.delete(objectKey);
        } catch (IOException exc) {
            throw new DatabaseException(exc);
        }
    }
}
