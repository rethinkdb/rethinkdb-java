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
}



