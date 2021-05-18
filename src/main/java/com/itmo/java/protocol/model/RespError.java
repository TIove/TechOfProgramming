package com.itmo.java.protocol.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';

    private final byte[] message;

    public RespError(byte[] message) {
        this.message = message;
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        return new String(message, StandardCharsets.UTF_8);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        stream.write(CODE);
        stream.write(message);
        stream.write(CRLF);

        stream.writeTo(os);
    }
}
