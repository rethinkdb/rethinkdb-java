package com.rethinkdb.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.*;
import com.rethinkdb.gen.exc.ReqlDriverCompileError;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.*;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Map;
import java.util.*;
import java.util.stream.Collectors;

/**
 * RethinkDB Common Internals.
 * Methods and fields are subject to change at any moment.
 */
public class Internals {
    private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
    private static final TypeReference<Map<String, Object>> mapTypeRef = Types.mapOf(String.class, Object.class);
    private static final ObjectMapper internalMapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.USE_LONG_FOR_INTS);
    private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");


    public static final String PSEUDOTYPE_KEY = "$reql_type$";
    public static final String TIME = "TIME";
    public static final String GROUPED_DATA = "GROUPED_DATA";
    public static final String GEOMETRY = "GEOMETRY";
    public static final String BINARY = "BINARY";

    public static int startingDepth = 100;

    private Internals() {
    }

    public static ObjectMapper getInternalMapper() {
        return internalMapper;
    }

    public static Map<String, Object> asBinaryPseudotype(byte[] data) {
        return new MapObject<String, Object>()
            .with("$reql_type$", BINARY)
            .with("data", Base64.getEncoder().encodeToString(data));
    }

    public static Map<String, Object> readJson(ByteBuffer buf) {
        try {
            return getInternalMapper().readValue(
                buf.array(),
                buf.arrayOffset() + buf.position(),
                buf.remaining(),
                mapTypeRef
            );
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    public static Map<String, Object> readJson(String str) {
        try {
            return getInternalMapper().readValue(str, mapTypeRef);
        } catch (IOException e) {
            throw new ReqlDriverError(e);
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

    @SuppressWarnings("unchecked")
    public static <T> T toPojo(Object value, TypeReference<T> typeRef) {
        if (typeRef != null) {
            JavaType type = getInternalMapper().getTypeFactory().constructType(typeRef);
            Class<T> rawClass = (Class<T>) type.getRawClass();
            if (rawClass.isEnum()) {
                Enum<?>[] enumConstants = ((Class<Enum<?>>) rawClass).getEnumConstants();
                for (Enum<?> enumConst : enumConstants) {
                    if (enumConst.name().equals(value)) {
                        return (T) enumConst;
                    }
                }
            } else if (rawClass.isAssignableFrom(value.getClass()) && type.containedTypeCount() == 0) {
                // class is assignable from value and has no type parameters.
                // since the only thing that matches those are primitives, strings and dates, it's safe
                return rawClass.cast(value);
            } else {
                try {
                    return RethinkDB.getResultMapper().convertValue(value, typeRef);
                } catch (Exception e) {
                    throw new ReqlDriverError("Tried to convert " + value + ", of type " + value.getClass() + ", to " + typeRef.getType() + ", but got " + e, e);
                }
            }
        }
        return (T) value;
    }

    public static ReqlAst toReqlAst(Object val) {
        return toReqlAst(val, startingDepth);
    }

    public static ReqlExpr toReqlExpr(Object val) {
        ReqlAst converted = toReqlAst(val);
        if (converted instanceof ReqlExpr) {
            return (ReqlExpr) converted;
        } else {
            throw new ReqlDriverError("Cannot convert %s to ReqlExpr", val);
        }
    }

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
                    .map(it -> new ArrayList<>((Collection<?>) it))
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

    private static ReqlAst toReqlAst(Object val, int remainingDepth) {
        if (val instanceof ReqlAst) {
            return (ReqlAst) val;
        }

        if (val == null || val instanceof Number || val instanceof Boolean || val instanceof String) {
            return new Datum(val);
        }

        if (val instanceof LocalDateTime) {
            ZoneId zid = ZoneId.systemDefault();
            DateTimeFormatter fmt2 = fmt.withZone(zid);
            return Iso8601.fromString(((LocalDateTime) val).format(fmt2));
        }

        if (val instanceof ZonedDateTime || val instanceof OffsetDateTime) {
            return Iso8601.fromString(fmt.format(((Temporal) val)));
        }

        Class<?> valClass = val.getClass();
        if (valClass.isEnum()) {
            return new Datum(((Enum<?>) val).name());
        }

        if (remainingDepth <= 0) {
            throw new ReqlDriverCompileError("Recursion limit reached converting to ReqlAst");
        }

        if (val instanceof ReqlLambda) {
            return Func.fromLambda((ReqlLambda) val);
        }

        if (val instanceof Collection) {
            Arguments innerValues = new Arguments();
            for (Object innerValue : (Collection<?>) val) {
                innerValues.add(toReqlAst(innerValue, remainingDepth - 1));
            }
            return new MakeArray(innerValues, null);
        }

        if (val instanceof Map) {
            Map<String, ReqlAst> obj = new MapObject<>();
            ((Map<?, ?>) val).forEach((key, value) -> {
                if (key.getClass().isEnum()) {
                    obj.put(((Enum<?>) key).name(), toReqlAst(value, remainingDepth - 1));
                } else if (key instanceof String) {
                    obj.put((String) key, toReqlAst(value, remainingDepth - 1));
                } else {
                    throw new ReqlDriverCompileError("Object keys can only be strings");
                }
            });
            return MakeObj.fromMap(obj);
        }

        if (valClass.isArray()) {
            if (val instanceof byte[]) {
                return new Binary(((byte[]) val));
            }
            Arguments innerValues = new Arguments();
            int length = Array.getLength(val);
            for (int i = 0; i < length; i++) {
                innerValues.add(toReqlAst(Array.get(val, i)));
            }
            return new MakeArray(innerValues, null);
        }

        // val is a non-null POJO, let's use jackson
        return toReqlAst(RethinkDB.getResultMapper().convertValue(val, Map.class), remainingDepth - 1);
    }

    public static SSLContext readCertFile(@NotNull InputStream certFile) {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(certFile);

            ks.load(null);
            ks.setCertificateEntry("caCert", cert);
            tmf.init(ks);

            SSLContext ctx = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);
            ctx.init(null, tmf.getTrustManagers(), null);
            certFile.close();
            return ctx;
        } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new ReqlDriverError(e);
        }
    }

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
}
