package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {

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
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() == 5) {
            this.id = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString();
            this.commandName = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();

            this.environment = env;

            if (this.id == null ||
                    this.commandName == null ||
                    this.databaseName == null ||
                    this.tableName == null ||
                    this.environment == null ||
                    this.key == null) {
                throw new IllegalArgumentException("One or few arguments are null");
            }
        } else {
            throw new IllegalArgumentException("Incorrect argument count");
        }
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            if (environment.getDatabase(databaseName).isEmpty())
                return DatabaseCommandResult.error("DataBase " + databaseName + " doesn't exist");

            Optional<byte[]> value = environment.getDatabase(databaseName).get().read(tableName, key);

            if (value.isPresent()) {
                return DatabaseCommandResult.success(value.get());
            } else {
                return DatabaseCommandResult.success(null);
            }
        } catch (DatabaseException exc) {
            return DatabaseCommandResult.error(exc);
        }
    }
}
