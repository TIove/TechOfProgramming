package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Builder
@AllArgsConstructor
public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path rootPath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.rootPath = tablePath;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return rootPath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }
}
