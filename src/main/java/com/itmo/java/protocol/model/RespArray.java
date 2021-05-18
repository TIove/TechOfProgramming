package com.itmo.java.protocol.model;

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
        }

        return response.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        String str = CODE + objects.length + new String(CRLF, StandardCharsets.UTF_8);
        os.write(str.getBytes(StandardCharsets.UTF_8));
        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return Arrays.asList(objects);
    }
}
