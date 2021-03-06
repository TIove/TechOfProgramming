package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Builder
@AllArgsConstructor
public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final ExecutionEnvironment env;
    private final InitializationContext context;
    private final DatabaseServerInitializer initializer;
    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        var context = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();

        initializer.perform(context);

        return DatabaseServer.builder()
                .env(env)
                .initializer(initializer)
                .build();
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            var commandName = message.getObjects().get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
            var databaseCommands = DatabaseCommands.valueOf(commandName.asString());
            var command = databaseCommands.getCommand(env, message.getObjects());

            return command.execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute);
    }
}