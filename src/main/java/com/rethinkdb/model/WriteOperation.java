package com.rethinkdb.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO describing results of a write operation: insert, update, delete or
 * replace.
 */
public class WriteOperation {
    @JsonProperty("generated_keys")
    private final List<String> generatedKeys = new ArrayList<>();
    private final List<Change> changes = new ArrayList<>();
    private int inserted, replaced, unchanged, deleted, skipped, errors;
    @JsonProperty("first_error")
    private String firstError;
    private String warnings;

    /**
     * @return the number of documents successfully inserted
     */
    public int getInserted() {
        return inserted;
    }

    public void setInserted(int inserted) {
        this.inserted = inserted;
    }

    /**
     * @return the number of documents updated
     */
    public int getReplaced() {
        return replaced;
    }

    public void setReplaced(int replaced) {
        this.replaced = replaced;
    }

    /**
     * @return the number of documents that would have been modified except the new
     *         value was the same as the old value
     */
    public int getUnchanged() {
        return unchanged;
    }

    public void setUnchanged(int unchanged) {
        this.unchanged = unchanged;
    }

    /**
     * @return the number of documents that were deleted
     */
    public int getDeleted() {
        return deleted;
    }

    public void setDeleted(int deleted) {
        this.deleted = deleted;
    }

    /**
     * @return the number of documents that were skipped
     */
    public int getSkipped() {
        return skipped;
    }

    public void setSkipped(int skipped) {
        this.skipped = skipped;
    }

    /**
     * @return the number of errors encountered while performing the operation
     */
    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    /**
     * @return if errors were encountered, contains the text of the first error,
     *         {@code null} otherwise
     */
    public String getFirstError() {
        return firstError;
    }

    public void setFirstError(String firstError) {
        this.firstError = firstError;
    }

    /**
     * @return only on insert, if the field {@code generatedKeys} is truncated, you
     *         will get the warning
     *         {@code "Too many generated keys (<X>), array truncated
     *         to 100000."}
     */
    public String getWarnings() {
        return warnings;
    }

    public void setWarnings(String warnings) {
        this.warnings = warnings;
    }

    /**
     * @return if {@code returnChanges} is set to {@code true}, this will be an
     *         array of objects, one for each object affected by the operation.
     */
    public List<Change> getChanges() {
        return changes;
    }

    /**
     * @return a list of generated primary keys for insered documents whose primary
     *         keys were not specified (capped to 100,000).
     */
    public List<String> getGeneratedKeys() {
        return generatedKeys;
    }

    @Override
    public int hashCode() {
        return Objects.hash(changes, deleted, errors, firstError, generatedKeys, inserted, replaced, skipped, unchanged,
                warnings);
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
        WriteOperation other = (WriteOperation) obj;
        return Objects.equals(changes, other.changes) && deleted == other.deleted && errors == other.errors
                && Objects.equals(firstError, other.firstError) && Objects.equals(generatedKeys, other.generatedKeys)
                && inserted == other.inserted && replaced == other.replaced && skipped == other.skipped
                && unchanged == other.unchanged && Objects.equals(warnings, other.warnings);
    }

    @Override
    public String toString() {
        return "WriteOperation{generatedKeys="
                + generatedKeys
                + ", changes="
                + changes
                + ", inserted="
                + inserted
                + ", replaced="
                + replaced
                + ", unchanged="
                + unchanged
                + ", deleted="
                + deleted
                + ", skipped="
                + skipped
                + ", errors="
                + errors
                + ", firstError="
                + firstError
                + ", warnings="
                + warnings
                + "}";
    }
}
