// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/Exception.java
package com.rethinkdb.gen.exc;

import org.jetbrains.annotations.Nullable;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.model.Backtrace;

public class ReqlNonExistenceError extends ReqlQueryLogicError {

    @Nullable Backtrace backtrace;
    @Nullable ReqlAst term;

    public ReqlNonExistenceError() {
    }

    public ReqlNonExistenceError(String message) {
        super(message);
    }

    public ReqlNonExistenceError(String format, Object... args) {
        super(String.format(format, args));
    }

    public ReqlNonExistenceError(String message, Throwable cause) {
        super(message, cause);
    }

    public ReqlNonExistenceError(Throwable cause) {
        super(cause);
    }

    public ReqlNonExistenceError(String msg, ReqlAst term, Backtrace bt) {
        super(msg);
        this.backtrace = bt;
        this.term = term;
    }

    public ReqlNonExistenceError setBacktrace(Backtrace backtrace) {
        this.backtrace = backtrace;
        return this;
    }

    public @Nullable Backtrace getBacktrace() {
        return backtrace;
    }

    public ReqlNonExistenceError setTerm(ReqlAst term) {
        this.term = term;
        return this;
    }

    public @Nullable ReqlAst getTerm() {
        return this.term;
    }
}
