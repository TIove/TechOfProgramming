package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    public final String id;
    public final String commandName;
    private final String databaseName;
    private final String tableName;
    private final String key;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.contains(null)) {
            throw new IllegalArgumentException("One or few arguments are null");
        }

        if (commandArgs.size() == 5) {
            this.id = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString();
            this.commandName = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();

            this.environment = env;
        } else {
            throw new IllegalArgumentException("Incorrect argument count");
        }
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            if (environment.getDatabase(databaseName).isEmpty())
                return DatabaseCommandResult.error("DataBase " + databaseName + " doesn't exist"); //TODO hz

            Optional<byte[]> previousValue = environment.getDatabase(databaseName).get().read(tableName, key);
            if (previousValue.isEmpty())
                return DatabaseCommandResult.error("Cannot get value by key = " + key);

            environment.getDatabase(databaseName).get().delete(tableName, key); // TODO hz

            return DatabaseCommandResult.success(previousValue.get());
        } catch (DatabaseException exc) {
            return DatabaseCommandResult.error(exc);
        }
    }
}
