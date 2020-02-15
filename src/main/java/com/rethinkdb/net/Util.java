package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Util {
    private Util() {}
    public static long deadline(long timeout) {
        return System.currentTimeMillis() + timeout;
    }

    public static ByteBuffer leByteBuffer(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        // Creating the ByteBuffer over an underlying array makes
        // it easier to turn into a string later.
        //return ByteBuffer.wrap(new byte[capacity]).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static String bufferToString(ByteBuffer buf) {
        return new String(
            buf.array(),
            buf.arrayOffset() + buf.position(),
            buf.remaining(),
            StandardCharsets.UTF_8
        );
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toJSON(String str) {
        try {
            return RethinkDB.getInternalMapper().readValue(str, Map.class);
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toJSON(ByteBuffer buf) {
        try {
            return RethinkDB.getInternalMapper().readValue(
                buf.array(),
                buf.arrayOffset() + buf.position(),
                buf.remaining(),
                Map.class
            );
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertToPojo(Object value, TypeReference<T> typeRef) {
        if (typeRef != null) {
            Class<?> rawClass = RethinkDB.getInternalMapper().getTypeFactory().constructType(typeRef).getRawClass();
            if (rawClass.isEnum()) {
                Enum<?>[] enumConstants = ((Class<Enum<?>>) rawClass).getEnumConstants();
                for (Enum<?> enumConst : enumConstants) {
                    if (enumConst.name().equals(value)) {
                        return (T) enumConst;
                    }
                }
            } else if (value instanceof Map) {
                return (T) RethinkDB.getPOJOMapper().convertValue(value, typeRef);
            }
        }
        return (T) value;
    }

    public static byte[] toUTF8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromUTF8(byte[] ba) {
        return new String(ba, StandardCharsets.UTF_8);
    }
}



