package com.rethinkdb.model;

import java.util.LinkedHashMap;

public class MapObject<K, V> extends LinkedHashMap<K, V> {

    public MapObject() {
    }

    public MapObject<K, V> with(K key, V value) {
        put(key, value);
        return this;
    }
}
