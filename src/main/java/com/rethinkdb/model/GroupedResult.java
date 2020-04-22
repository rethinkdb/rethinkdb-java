package com.rethinkdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

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

    public static <G, V> Map<G, Set<V>> toMap(List<GroupedResult<G, V>> list) {
        return list.stream().collect(Collectors.toMap(GroupedResult::getGroup, it -> new LinkedHashSet<>(it.getValues())));
    }

    @Override
    public String toString() {
        return "GroupedResult{" +
            "group=" + group +
            ", values=" + values +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupedResult<?, ?> that = (GroupedResult<?, ?>) o;
        return Objects.equals(group, that.group) &&
            Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, values);
    }
}
