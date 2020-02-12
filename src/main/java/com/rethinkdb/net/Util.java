package com.rethinkdb.net;

import com.rethinkdb.RethinkDB;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Util {

    public static long deadline(long timeout) {
        return System.currentTimeMillis() + timeout;
    }

    private static Logger log = LoggerFactory.getLogger(Util.class);
    public static ByteBuffer leByteBuffer(int capacity) {
        // Creating the ByteBuffer over an underlying array makes
        // it easier to turn into a string later.
        byte[] underlying = new byte[capacity];
        return ByteBuffer.wrap(underlying)
                .order(ByteOrder.LITTLE_ENDIAN);
    }

    public static String bufferToString(ByteBuffer buf) {
        // This should only be used on ByteBuffers we've created by
        // wrapping an array
        return new String(buf.array(), StandardCharsets.UTF_8);
    }

    public static JSONObject toJSON(String str) {
        return (JSONObject) JSONValue.parse(str);
    }

    public static JSONObject toJSON(ByteBuffer buf) {
        InputStreamReader codepointReader =
                new InputStreamReader(new ByteArrayInputStream(buf.array()));
        return (JSONObject) JSONValue.parse(codepointReader);
    }

    public static <T, P> T convertToPojo(Object value, Optional<Class<P>> pojoClass) {
        return !pojoClass.isPresent() || !(value instanceof Map)
                ? (T) value
                : (T) RethinkDB.getObjectMapper().convertValue(value, pojoClass.get());
    }

    public static byte[] toUTF8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromUTF8(byte[] ba) {
        return new String(ba, StandardCharsets.UTF_8);
    }
}



