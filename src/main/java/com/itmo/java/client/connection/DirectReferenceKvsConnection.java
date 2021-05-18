package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {

    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {
        RespArray newCommand = new RespArray(new RespCommandId(commandId), command);

        DatabaseCommandResult result;
        try {
            result = databaseServer.executeNextCommand(newCommand).get();
        } catch( InterruptedException | ExecutionException exception) {
            throw new ConnectionException("Cannot read answer", exception);
        }

        return result.serialize();
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}
