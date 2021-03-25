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
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private static final int MAX_SEGMENT_SIZE = 100_000;

    private boolean _isReadOnly = false;
    private int _segmentSize;

    private final String _name;
    private final Path _segmentFullPath;

    private final DatabaseOutputStream _dbOutputStream;

    private final SegmentIndex _segmentIndex = new SegmentIndex();

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        _segmentSize = 0;
        _name = segmentName;
        _segmentFullPath = Path.of(tableRootPath.toString() + '/' + segmentName);

        try {
            _dbOutputStream = createDatabaseOutputStream(_segmentFullPath);
        } catch (IOException exc) {
            throw new DatabaseException("Exception while creating stream for path - "
                    + _segmentFullPath.toString(), exc);
        }
    }

    private DatabaseOutputStream createDatabaseOutputStream(Path path) throws IOException {
        DataOutputStream outputStream =
                new DataOutputStream(new FileOutputStream(path.toString(), true));

        return new DatabaseOutputStream(outputStream);
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
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

        var keyInBytes = objectKey.getBytes(StandardCharsets.UTF_8);

        WritableDatabaseRecord record;
        if (objectValue != null) {
            record = new SetDatabaseRecord(keyInBytes, objectValue);
        } else {
            record = new RemoveDatabaseRecord(keyInBytes);
        }

        int currentOffset = _dbOutputStream.write(record);

        var offsetInfo = new SegmentOffsetInfoImpl(_segmentSize);
        _segmentIndex.onIndexedEntityUpdated(objectKey, offsetInfo);
        _segmentSize += currentOffset;

        if (MAX_SEGMENT_SIZE <= _segmentSize) {
            _isReadOnly = true;
            _dbOutputStream.close();
        }

        return !_isReadOnly;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var segment = _segmentIndex.searchForKey(objectKey);

        if (segment.isPresent()) {
            long offsetToRecord = segment.get().getOffset();
            try (FileInputStream fileInputStream = new FileInputStream(_segmentFullPath.toString());
                 DataInputStream dataInputStream = new DataInputStream(fileInputStream);
                 DatabaseInputStream inputStream = new DatabaseInputStream(dataInputStream)) {

                if (inputStream.skip(offsetToRecord) != offsetToRecord) {
                    inputStream.close();
                    return Optional.empty();
                }

                var dbRecord = inputStream.readDbUnit();
                inputStream.close();

                if (dbRecord.isPresent() && dbRecord.get().getValue() != null) {
                    return Optional.of(dbRecord.get().getValue());
                }
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
