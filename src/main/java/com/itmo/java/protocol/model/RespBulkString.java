package com.itmo.java.protocol.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    private final byte[] data;

    public RespBulkString(byte[] data) {
        this.data = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        if (data.length == 0)
            return null;

        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        if (data == null || data.length == 0) {
            stream.write(CODE);
            stream.write(NULL_STRING_SIZE);
            stream.write(CRLF);
        } else {
            stream.write(CODE);
            stream.write(data.length);
            stream.write(CRLF);
            stream.write(data);
            stream.write(CRLF);
        }

        stream.writeTo(os);
    }
}
