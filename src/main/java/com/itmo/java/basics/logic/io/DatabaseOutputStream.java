package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтахб используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public int write(WritableDatabaseRecord databaseRecord) throws IOException {
        int keySize = databaseRecord.getKeySize();
        int valueSize = databaseRecord.getValueSize();

        this.writeInt(keySize);
        this.write(databaseRecord.getKey());
        this.writeInt(valueSize);

        byte[] value = databaseRecord.getValue();
        if (value != null) {
            this.write(value);
        }

        return this.written;
    }
}