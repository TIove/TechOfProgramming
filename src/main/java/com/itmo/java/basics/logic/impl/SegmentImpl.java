package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

@Builder
@AllArgsConstructor
public class SegmentImpl implements Segment {
    private static final int MAX_SEGMENT_SIZE = 100_000;

    private boolean isReadOnly = false;
    private int segmentSize;

    private final String name;
    private final Path segmentFullPath;

    private final DatabaseOutputStream dbOutputStream;

    private final SegmentIndex segmentIndex;

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        segmentSize = 0;
        name = segmentName;
        segmentFullPath = Path.of(tableRootPath.toString() + File.separator + segmentName);
        segmentIndex = new SegmentIndex();

        try {
            dbOutputStream = createDatabaseOutputStream(segmentFullPath);
        } catch (IOException exc) {
            throw new DatabaseException("Exception while creating stream for path - "
                    + segmentFullPath, exc);
        }
    }

    private DatabaseOutputStream createDatabaseOutputStream(Path path) throws IOException {
        DataOutputStream outputStream =
                new DataOutputStream(new FileOutputStream(path.toString(), true));

        return new DatabaseOutputStream(outputStream);
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        int segmentSize = (int) context.getCurrentSize();
        String segmentName = context.getSegmentName();
        boolean isReadOnly = segmentSize >= MAX_SEGMENT_SIZE;
        Path segmentFullPath = context.getSegmentPath();
        SegmentIndex segmentIndex = context.getIndex();

        return SegmentImpl.builder()
                .segmentSize(segmentSize)
                .name(segmentName)
                .isReadOnly(isReadOnly) //TODO
                .segmentFullPath(segmentFullPath)
                .segmentIndex(segmentIndex)
                .build();
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()) {
            return false;
        }

//        var keyInBytes = objectKey.getBytes(StandardCharsets.UTF_8);
//
//        WritableDatabaseRecord record;
//        if (objectValue != null) {
//            record = new SetDatabaseRecord(keyInBytes, objectValue);
//        } else {
//            record = new RemoveDatabaseRecord(keyInBytes);
//        }
//
//        int currentOffset = dbOutputStream.write(record);
//
//        var offsetInfo = new SegmentOffsetInfoImpl(segmentSize);
//        segmentIndex.onIndexedEntityUpdated(objectKey, offsetInfo);
//        segmentSize += currentOffset;
//
//        if (segmentSize >= MAX_SEGMENT_SIZE) {
//            isReadOnly = true;
//            dbOutputStream.close();
//        }

        WritableDatabaseRecord record;
        if (objectValue == null) {
            record = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));
        } else {
            record = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        }

        if (record.size() + segmentSize > MAX_SEGMENT_SIZE) {
            segmentSize += this.dbOutputStream.write(record);

            return false;
        }
        else {
            segmentSize += this.dbOutputStream.write(record);

            return !isReadOnly;
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var segment = segmentIndex.searchForKey(objectKey);

        if (segment.isPresent()) {
            long offsetToRecord = segment.get().getOffset();
            try (FileInputStream fileInputStream = new FileInputStream(segmentFullPath.toString());
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
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (this.isReadOnly()) {
            return false;
        }

        write(objectKey, null);

        return true;
    }
}
