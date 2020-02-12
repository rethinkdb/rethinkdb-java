package com.rethinkdb.net;

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
    public static <T, P> T convertToPojo(Object value, Class<P> pojoClass) {
        return pojoClass == null || !(value instanceof Map)
            ? (T) value
            : (T) RethinkDB.getPOJOMapper().convertValue(value, pojoClass);
    }

    public static byte[] toUTF8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromUTF8(byte[] ba) {
        return new String(ba, StandardCharsets.UTF_8);
    }
}



