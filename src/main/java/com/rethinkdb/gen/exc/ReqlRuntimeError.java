// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/Exception.java
package com.rethinkdb.gen.exc;

import org.jetbrains.annotations.Nullable;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.model.Backtrace;

public class ReqlRuntimeError extends ReqlError {

    @Nullable Backtrace backtrace;
    @Nullable ReqlAst term;

    public ReqlRuntimeError() {
    }

    public ReqlRuntimeError(String message) {
        super(message);
    }

    public ReqlRuntimeError(String format, Object... args) {
        super(String.format(format, args));
    }

    public ReqlRuntimeError(String message, Throwable cause) {
        super(message, cause);
    }

    public ReqlRuntimeError(Throwable cause) {
        super(cause);
    }

    public ReqlRuntimeError(String msg, ReqlAst term, Backtrace bt) {
        super(msg);
        this.backtrace = bt;
        this.term = term;
    }

    public ReqlRuntimeError setBacktrace(Backtrace backtrace) {
        this.backtrace = backtrace;
        return this;
    }

    public @Nullable Backtrace getBacktrace() {
        return backtrace;
    }

    public ReqlRuntimeError setTerm(ReqlAst term) {
        this.term = term;
        return this;
    }

    public @Nullable ReqlAst getTerm() {
        return this.term;
    }
}
