package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class Util {
    private static final TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<Map<String, Object>>() {};

    private Util() {}

    public static Map<String, Object> readJSON(String str) {
        try {
            return RethinkDB.getInternalMapper().readValue(str, mapTypeRef);
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    public static Map<String, Object> readJSON(ByteBuffer buf) {
        try {
            return RethinkDB.getInternalMapper().readValue(
                buf.array(),
                buf.arrayOffset() + buf.position(),
                buf.remaining(),
                mapTypeRef
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
            } else {
                return RethinkDB.getResultMapper().convertValue(value, typeRef);
            }
        }
        return (T) value;
    }
}



