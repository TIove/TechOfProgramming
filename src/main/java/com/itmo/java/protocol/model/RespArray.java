package com.itmo.java.protocol.model;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final RespObject[] objects;

    public RespArray(RespObject... objects) {
        this.objects = objects;
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringBuilder response = new StringBuilder();

        for (RespObject object : objects) {
            response.append(object.asString());
            response.append(" ");
        }

        return response.toString().trim();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        writeInt(objects.length, os);
        os.write(CRLF);

        for (RespObject object : objects) {
            object.write(os);
        }
    }

    private void writeInt(int v, OutputStream out) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>>  8) & 0xFF);
        out.write((v >>>  0) & 0xFF);
    }

    public List<RespObject> getObjects() {
        return List.of(objects);
    }
}
