package com.rethinkdb.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.model.GroupedResult;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

/**
 * An utility class that can create simple generic type references without having to grab information from the compiler.
 */
public class Types {
    private Types() {
    }

    public static <T> TypeReference<T> of(Class<T> type) {
        return new BuiltTypeRef<>(Objects.requireNonNull(type, "type"));
    }

    public static <T> TypeReference<List<T>> listOf(Class<T> type) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(List.class, Objects.requireNonNull(type, "type"))
        );
    }

    public static <T> TypeReference<List<T>> listOf(TypeReference<T> type) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(List.class, Objects.requireNonNull(type, "type").getType())
        );
    }

    public static <T> TypeReference<Set<T>> setOf(Class<T> type) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Set.class, Objects.requireNonNull(type, "type"))
        );
    }

    public static <T> TypeReference<Set<T>> setOf(TypeReference<T> type) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Set.class, Objects.requireNonNull(type, "type").getType())
        );
    }

    public static <K, V> TypeReference<Map<K, V>> mapOf(Class<K> keyType, Class<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Map.class, Objects.requireNonNull(keyType, "keyType"), Objects.requireNonNull(valueType, "valueType"))
        );
    }

    public static <K, V> TypeReference<Map<K, V>> mapOf(TypeReference<K> keyType, Class<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Map.class, Objects.requireNonNull(keyType, "keyType").getType(), Objects.requireNonNull(valueType, "valueType"))
        );
    }

    public static <K, V> TypeReference<Map<K, V>> mapOf(Class<K> keyType, TypeReference<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Map.class, Objects.requireNonNull(keyType, "keyType"), Objects.requireNonNull(valueType, "valueType").getType())
        );
    }

    public static <K, V> TypeReference<Map<K, V>> mapOf(TypeReference<K> keyType, TypeReference<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(Map.class, Objects.requireNonNull(keyType, "keyType").getType(), Objects.requireNonNull(valueType, "valueType").getType())
        );
    }

    public static <K, V> TypeReference<GroupedResult<K, V>> groupOf(Class<K> keyType, Class<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(GroupedResult.class, Objects.requireNonNull(keyType, "keyType"), Objects.requireNonNull(valueType, "valueType"))
        );
    }

    public static <K, V> TypeReference<GroupedResult<K, V>> groupOf(TypeReference<K> keyType, Class<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(GroupedResult.class, Objects.requireNonNull(keyType, "keyType").getType(), Objects.requireNonNull(valueType, "valueType"))
        );
    }

    public static <K, V> TypeReference<GroupedResult<K, V>> groupOf(Class<K> keyType, TypeReference<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(GroupedResult.class, Objects.requireNonNull(keyType, "keyType"), Objects.requireNonNull(valueType, "valueType").getType())
        );
    }

    public static <K, V> TypeReference<GroupedResult<K, V>> groupOf(TypeReference<K> keyType, TypeReference<V> valueType) {
        return new BuiltTypeRef<>(
            new BuiltParametrizedType(GroupedResult.class, Objects.requireNonNull(keyType, "keyType").getType(), Objects.requireNonNull(valueType, "valueType").getType())
        );
    }

    private static class BuiltTypeRef<T> extends TypeReference<T> {
        private final Type type;

        private BuiltTypeRef(Type type) {
            this.type = type;
        }

        @Override
        public Type getType() {
            return type;
        }
    }

    private static class BuiltParametrizedType implements ParameterizedType {
        private final Class<?> type;
        private final Type[] params;

        public BuiltParametrizedType(Class<?> type, Type... params) {
            if (params.length != type.getTypeParameters().length) {
                throw new MalformedParameterizedTypeException();
            }
            this.type = type;
            this.params = params;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return params.clone();
        }

        @Override
        public Type getRawType() {
            return type;
        }

        @Override
        public Type getOwnerType() {
            return type.getDeclaringClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ParameterizedType)) return false;
            ParameterizedType that = (ParameterizedType) o;
            return Objects.equals(type, that.getRawType()) &&
                Objects.equals(type.getDeclaringClass(), that.getOwnerType()) &&
                Arrays.equals(params, that.getActualTypeArguments());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(params) ^ Objects.hashCode(type) ^ Objects.hashCode(type.getDeclaringClass());
        }

        @Override
        public String toString() {
            return type.getName() + Arrays.stream(params).map(Type::getTypeName)
                .collect(Collectors.joining(", ", "<", ">"));
        }
    }
}
