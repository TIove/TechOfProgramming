package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    private String executeCommand(KvsCommand command, String exceptionString) throws DatabaseExecutionException {
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException exc) {
            throw new DatabaseExecutionException(exceptionString, exc);
        }

        if (result.isError())  {
            throw new DatabaseExecutionException(result.asString());
        }

        return result.asString();
    }

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand command = new CreateDatabaseKvsCommand(databaseName);

        return executeCommand(
                command,
                "Exception appears while of creating database");
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);

        return executeCommand(
                command,
                "Exception appears while of creating table " + tableName);
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(databaseName, tableName, key);

        return executeCommand(
                command,
                "Exception appears while of getting value by key = " + key + " from table " + tableName);
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);

        return executeCommand(
                command,
                "Exception appears while of setting value with key = " + key + " in table " + tableName);
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);

        return executeCommand(
                command,
                "Exception appears while of deleting value by key = " + key + " from table " + tableName);
    }
}
