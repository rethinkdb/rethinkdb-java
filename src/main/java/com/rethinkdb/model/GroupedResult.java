package com.rethinkdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GroupedResult<G, V> {
    private final G group;
    private final List<V> values;

    @JsonCreator
    public GroupedResult(@JsonProperty("group") G group, @JsonProperty("values") List<V> values) {
        this.group = group;
        this.values = values;
    }

    public G getGroup() {
        return group;
    }

    public List<V> getValues() {
        return values;
    }
}
