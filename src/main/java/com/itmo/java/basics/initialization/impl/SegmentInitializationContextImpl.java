package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.File;
import java.nio.file.Path;

@Builder
@AllArgsConstructor
public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    public static final int DEFAULT_INDEX_SIZE = 0;

    private final String segmentName;
    private final int currentSize;

    private final Path segmentPath;
    private final SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = Path.of(tablePath.toString() + File.separator + segmentName);
        this.currentSize = currentSize;
        this.index = new SegmentIndex();
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return this.segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return this.index;
    }

    @Override
    public long getCurrentSize() {
        return this.currentSize;
    }
}
