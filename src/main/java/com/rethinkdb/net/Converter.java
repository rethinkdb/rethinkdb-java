package com.rethinkdb.net;


import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.GroupedResult;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.OptArgs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class Converter {
    public static final String PSEUDOTYPE_KEY = "$reql_type$";
    public static final String TIME = "TIME";
    public static final String GROUPED_DATA = "GROUPED_DATA";
    public static final String GEOMETRY = "GEOMETRY";
    public static final String BINARY = "BINARY";

    /* Compact way of keeping these flags around through multiple recursive passes */
    public static class FormatOptions {
        public final boolean rawTime;
        public final boolean rawGroups;
        public final boolean rawBinary;

        public FormatOptions(OptArgs args) {
            Datum time_format = (Datum) args.get("time_format");
            this.rawTime = time_format != null && "raw".equals(time_format.datum);

            Datum binary_format = (Datum) args.get("binary_format");
            this.rawBinary = binary_format != null && "raw".equals(binary_format.datum);

            Datum group_format = (Datum) args.get("group_format");
            this.rawGroups = group_format != null && "raw".equals(group_format.datum);
        }
    }

    public static Object convertPseudotypes(Object obj, FormatOptions fmt) {
        if (obj instanceof List) {
            return ((List<?>) obj).stream()
                .map(item -> convertPseudotypes(item, fmt))
                .collect(Collectors.toList());
        }
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.containsKey(PSEUDOTYPE_KEY)) {
                return handlePseudotypes(map, fmt);
            }
            return map.entrySet().stream().collect(
                LinkedHashMap::new,
                (m, e) -> m.put(e.getKey(), convertPseudotypes(e.getValue(), fmt)),
                LinkedHashMap::putAll
            );
        }

        return obj;
    }

    //convertPseudotypes

    private static Object handlePseudotypes(Map<?, ?> value, FormatOptions fmt) {
        switch ((String) value.get(PSEUDOTYPE_KEY)) {
            case TIME: {
                if (fmt.rawTime) {
                    return value;
                }
                try {
                    long epochMillis = (long) (((Number) value.get("epoch_time")).doubleValue() * 1000.0);
                    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.of((String) value.get("timezone")));
                } catch (Exception ex) {
                    throw new ReqlDriverError("Error handling date", ex);
                }
            }
            case GROUPED_DATA: {
                if (fmt.rawGroups) {
                    return value;
                }
                return ((List<?>) value.get("data")).stream()
                    .map(it -> new ArrayList<>((List<?>) it))
                    .map(it -> new GroupedResult<>(it.remove(0), it))
                    .collect(Collectors.toList());
            }
            case BINARY: {
                if (fmt.rawBinary) {
                    return value;
                }
                return Base64.getMimeDecoder().decode((String) value.get("data"));
            }
            case GEOMETRY: {
                // Nothing specific here
                return value;
            }
        }
        return value;
    }

    public static Map<String, Object> toBinary(byte[] data) {
        return new MapObject<String, Object>()
            .with("$reql_type$", BINARY)
            .with("data", Base64.getMimeEncoder().encodeToString(data));
    }
}
