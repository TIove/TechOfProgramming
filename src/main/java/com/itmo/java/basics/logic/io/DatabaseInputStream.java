package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        int keyLength = super.readInt();
        byte[] key = super.in.readNBytes(keyLength);

        if (key.length != keyLength) {
            return Optional.empty();
        }

        int valueLength = super.readInt();

        if (valueLength == REMOVED_OBJECT_SIZE) {
            return Optional.empty();
        }

        byte[] value = super.in.readNBytes(valueLength);

        if (value.length != valueLength) {
            return Optional.empty();
        }

        return Optional.of(new SetDatabaseRecord(key, value));
    }
}
