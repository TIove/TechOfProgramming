package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
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
    private static final int MAX_SEGMENT_SIZE = 100000;

    private final String name;
    private final SegmentIndex index;
    private final Path filePath;
    private long offset = 0;

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        try {
            return new SegmentImpl(segmentName, tableRootPath);
        } catch (IOException e) {
            throw new DatabaseException("Segment creation error:  " + e.getMessage(), e);
        }
    }

    private SegmentImpl(String segmentName, Path tableRootPath) throws IOException {
        index = new SegmentIndex();
        name = segmentName;
        filePath = Path.of(tableRootPath.toString() + File.separator + name);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        int segmentSize = (int) context.getCurrentSize();
        String segmentName = context.getSegmentName();
        boolean isReadOnly = segmentSize >= MAX_SEGMENT_SIZE;
        Path segmentFullPath = context.getSegmentPath();
        SegmentIndex segmentIndex = context.getIndex();

        return SegmentImpl.builder()
                .offset(segmentSize)
                .name(segmentName)
                .filePath(segmentFullPath)
                .index(segmentIndex)
                .build();
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return filePath.getFileName().toString();
    }

    private int writeStream(WritableDatabaseRecord record) throws IOException {
        try (DatabaseOutputStream stream = new DatabaseOutputStream(new FileOutputStream(filePath.toString(), true))) {
            index.onIndexedEntityUpdated(new String(record.getKey()), new SegmentOffsetInfoImpl(offset));
            var returned = stream.write(record);
            stream.close();
            return returned;
        }
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        SetDatabaseRecord record = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        if (record.size() + offset > MAX_SEGMENT_SIZE) {
            offset += writeStream(record);
            return false;
        }
        offset += writeStream(record);
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        var currentOffset = index.searchForKey(objectKey);

        if (currentOffset.isEmpty())
            return Optional.empty();

        var fileStream = new FileInputStream(filePath.toString());

        fileStream.skip(currentOffset.get().getOffset());
        var stream = new DatabaseInputStream(fileStream);
        var readRecord = stream.readDbUnit();
        return readRecord.map(DatabaseRecord::getValue);
    }

    @Override
    public boolean isReadOnly() {
        return filePath.toFile().length() >= MAX_SEGMENT_SIZE;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (this.isReadOnly())
            return false;
        writeStream(new RemoveDatabaseRecord(objectKey.getBytes()));
        return true;
    }
}