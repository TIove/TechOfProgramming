package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";

    private final String databaseName;
    private final String tableName;
    private final int id;

    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.id = KvsCommand.idGen.getAndIncrement();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(
                new RespCommandId(getCommandId()),
                new RespBulkString(COMMAND_NAME.getBytes()),
                new RespBulkString(databaseName.getBytes()),
                new RespBulkString(tableName.getBytes()));
    }

    @Override
    public int getCommandId() {
        return id;
    }
}
