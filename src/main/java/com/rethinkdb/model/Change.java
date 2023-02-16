package com.rethinkdb.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Change {
    @JsonProperty("old_val")
    private Map<String, Object> oldValue = new HashMap<>();

    @JsonProperty("new_val")
    private Map<String, Object> newValue = new HashMap<>();


    public Map<String, Object> getOldValue() {
        return oldValue;
    }

    public void setOldValue(Map<String, Object> oldValue) {
        this.oldValue = oldValue;
    }

    public Map<String, Object> getNewValue() {
        return newValue;
    }

    public void setNewValue(Map<String, Object> newValue) {
        this.newValue = newValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(newValue, oldValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Change other = (Change) obj;
        return Objects.equals(newValue, other.newValue) && Objects.equals(oldValue, other.oldValue);
    }

    @Override
    public String toString() {
        return "Change{oldValue=" + oldValue + ", newValue=" + newValue + "}";
    }
}
