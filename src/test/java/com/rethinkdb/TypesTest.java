package com.rethinkdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.utils.Types;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TypesTest {
    private interface T {}
    private interface K {}
    private interface V {}

    private static final TypeReference<T> TYPE_REF_T = new TypeReference<T>() {};
    private static final TypeReference<K> TYPE_REF_K = new TypeReference<K>() {};
    private static final TypeReference<V> TYPE_REF_V = new TypeReference<V>() {};
    private static final TypeReference<List<V>> TYPE_REF_LIST_V = new TypeReference<List<V>>() {};
    private static final TypeReference<List<T>> TYPE_REF_LIST_T = new TypeReference<List<T>>() {};
    private static final TypeReference<Set<T>> TYPE_REF_SET_T = new TypeReference<Set<T>>() {};
    private static final TypeReference<Map<K, V>> TYPE_REF_MAP_K_V = new TypeReference<Map<K, V>>() {};
    private static final TypeReference<Map<K, List<V>>> TYPE_REF_MAP_K_LIST_V = new TypeReference<Map<K, List<V>>>() {};

    private static <T> void assertType(TypeReference<T> expected, TypeReference<T> actual) {
        assertEquals(expected.getType(), actual.getType());
    }

    @Test
    public void testOf() {
        assertType(TYPE_REF_T, Types.of(T.class));
    }

    @Test(expected = NullPointerException.class)
    public void testNullClassOf() {
        Types.of(null);
    }

    @Test
    public void testListOf() {
        assertType(TYPE_REF_LIST_T, Types.listOf(T.class));
        assertType(TYPE_REF_LIST_T, Types.listOf(TYPE_REF_T));
    }

    @Test(expected = NullPointerException.class)
    public void testNullClassListOf() {
        Types.listOf((Class<Object>) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTypeListOf() {
        Types.listOf((TypeReference<Object>) null);
    }

    @Test
    public void testSetOf() {
        assertType(TYPE_REF_SET_T, Types.setOf(T.class));
        assertType(TYPE_REF_SET_T, Types.setOf(TYPE_REF_T));
    }

    @Test(expected = NullPointerException.class)
    public void testNullClassSetOf() {
        Types.setOf((Class<Object>) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTypeSetOf() {
        Types.setOf((TypeReference<Object>) null);
    }

    @Test
    public void testMapOf() {
        assertType(TYPE_REF_MAP_K_V, Types.mapOf(K.class, V.class));
        assertType(TYPE_REF_MAP_K_V, Types.mapOf(TYPE_REF_K, V.class));
        assertType(TYPE_REF_MAP_K_V, Types.mapOf(K.class, TYPE_REF_V));
        assertType(TYPE_REF_MAP_K_V, Types.mapOf(TYPE_REF_K, TYPE_REF_V));

        assertType(TYPE_REF_MAP_K_LIST_V, Types.mapOf(K.class, Types.listOf(V.class)));
        assertType(TYPE_REF_MAP_K_LIST_V, Types.mapOf(TYPE_REF_K, Types.listOf(TYPE_REF_V)));
        assertType(TYPE_REF_MAP_K_LIST_V, Types.mapOf(K.class, TYPE_REF_LIST_V));
        assertType(TYPE_REF_MAP_K_LIST_V, Types.mapOf(TYPE_REF_K, TYPE_REF_LIST_V));
    }

    @Test(expected = NullPointerException.class)
    public void testNullClassKeyMapOf() {
        Types.mapOf((Class<Object>) null, V.class);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTypeKeyMapOf() {
        Types.mapOf((TypeReference<Object>) null, V.class);
    }

    @Test(expected = NullPointerException.class)
    public void testNullClassValueMapOf() {
        Types.mapOf(K.class, (Class<Object>) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullTypeValueMapOf() {
        Types.mapOf(K.class, (TypeReference<Object>) null);
    }
}