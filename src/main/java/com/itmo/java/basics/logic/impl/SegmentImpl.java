package com.itmo.java.basics.logic.impl;


import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private static final int MAX_SEGMENT_SIZE = 100_000;

    private boolean _isReadOnly = false;
    private int _segmentSize = 0;

    private String _name;
    private Path _segmentFullPath;

    private DataOutputStream _outputStream;
    private DataInputStream _inputStream;

    private SegmentIndex _segmentIndex = new SegmentIndex();


    private SegmentImpl(String segmentName, Path tableRootPath) {
        _name = segmentName;
        _segmentFullPath = Path.of(tableRootPath.toString() + '/' + segmentName);
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path fullPath;
        try {
            fullPath = Paths.get(tableRootPath.toString() + "/" + segmentName);
            Files.createFile(fullPath);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return new SegmentImpl(segmentName, tableRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()) {
            return false;
        }

        if (_outputStream == null) {
            _outputStream = new DataOutputStream(new FileOutputStream(_segmentFullPath.toString(), true));
        }
        DatabaseOutputStream outputStream = new DatabaseOutputStream(_outputStream);

        var keyInBytes = objectKey.getBytes(StandardCharsets.UTF_8);

        WritableDatabaseRecord record;
        if (objectValue != null) {
            record = new SetDatabaseRecord(keyInBytes, objectValue);
        } else {
            record = new RemoveDatabaseRecord(keyInBytes);
        }
        int currentOffset = outputStream.write(record);

        var offsetInfo = new SegmentOffsetInfoImpl(_segmentSize);
        _segmentIndex.onIndexedEntityUpdated(objectKey, offsetInfo);
        _segmentSize += currentOffset;

        if (MAX_SEGMENT_SIZE < _segmentSize)
            _isReadOnly = true;

        return !_isReadOnly;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var segment = _segmentIndex.searchForKey(objectKey);

        if (segment.isPresent()) {
            var neededOffset = segment.get().getOffset();
            _inputStream = new DataInputStream(new FileInputStream(_segmentFullPath.toString()));
            DatabaseInputStream inputStream = new DatabaseInputStream(_inputStream);
            if (inputStream.skip(neededOffset) != neededOffset) {
                _inputStream.close();
                inputStream.close();
                return Optional.empty();
            }

            var dbRecord = inputStream.readDbUnit();
            inputStream.close();
            _inputStream.close();
            if (dbRecord.isPresent() && dbRecord.get().getValue() != null) {
                return Optional.of(dbRecord.get().getValue());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean isReadOnly() {
        return _isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        return write(objectKey, null);
    }
}
