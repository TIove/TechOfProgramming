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
    private String _name;
    private Path _path;
    private Path _tableRootPath;
    private TableIndex _tableIndex;
    private Segment _currentSegment;

    private TableImpl(
            String tableName,
            Path pathToDatabaseRoot,
            Path tableRootPath,
            TableIndex tableIndex)
    {
        _name = tableName;
        _path = pathToDatabaseRoot;
        _tableRootPath = tableRootPath;
        _tableIndex = tableIndex;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path fullPath;
        try {
            fullPath = Paths.get(pathToDatabaseRoot.toString() + '/' + tableName);
            Files.createDirectory(fullPath);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return new TableImpl(tableName, pathToDatabaseRoot, fullPath, tableIndex);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (_tableIndex.searchForKey(objectKey).isPresent())
            throw new DatabaseException("This key already exists");

        try {
            if (_currentSegment == null) {
                var segmentName = SegmentImpl.createSegmentName(_name);
                _currentSegment = SegmentImpl.create(segmentName, _tableRootPath);
            }
            _tableIndex.onIndexedEntityUpdated(objectKey, _currentSegment);
            if (!_currentSegment.write(objectKey, objectValue)) {
                var segmentName = SegmentImpl.createSegmentName(_name);
                _currentSegment = SegmentImpl.create(segmentName, _tableRootPath);
            }
        } catch(IOException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = _tableIndex.searchForKey(objectKey);
        try {
            if (segment.isPresent())
                return segment.get().read(objectKey);
            else
                throw new DatabaseException("Segment was damaged");
        } catch (IOException exc) {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        var segment = _tableIndex.searchForKey(objectKey);

        if (segment.isPresent()) {
            try {
                segment.get().delete(objectKey);
            } catch (IOException exc) {
                throw new DatabaseException(exc);
            }
        } else {
            throw new DatabaseException("Incorrect key");
        }
    }
}
