package com.itmo.java.protocol.model;

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
        String response;

        if (data == null || data.length == 0) {
            response = CODE + NULL_STRING_SIZE + Arrays.toString(CRLF);
        } else {
            response = CODE + data.length + Arrays.toString(CRLF) + Arrays.toString(data) + Arrays.toString(CRLF);
        }

        os.write(response.getBytes(StandardCharsets.UTF_8));
    }
}
