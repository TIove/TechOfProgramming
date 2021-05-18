package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    public final String id;
    public final String commandName;
    private final String databaseName;
    private final String tableName;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() == 4) {
            this.id = commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString();
            this.commandName = commandArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();

            this.environment = env;

            if (this.id == null ||
                    this.commandName == null ||
                    this.databaseName == null ||
                    this.tableName == null ||
                    this.environment == null) {
                throw new IllegalArgumentException("One or few arguments are null");
            }
        } else {
            throw new IllegalArgumentException("Incorrect argument count");
        }
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            if (environment.getDatabase(databaseName).isEmpty())
                return DatabaseCommandResult.error("DataBase " + databaseName + " doesn't exist"); //TODO hz

            environment.getDatabase(databaseName).get().createTableIfNotExists(tableName); // TODO hz

            return DatabaseCommandResult
                    .success(("Table " + tableName + " in database " + databaseName + " created").getBytes());
        } catch (DatabaseException exc) {
            return DatabaseCommandResult.error(exc);
        }
    }
}
