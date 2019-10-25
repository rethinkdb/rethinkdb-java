// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/Exception.java
package com.rethinkdb.gen.exc;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.model.Backtrace;
import org.jetbrains.annotations.Nullable;

public class ReqlAuthError extends ReqlDriverError {

    @Nullable Backtrace backtrace;
    @Nullable ReqlAst term;

    public ReqlAuthError() {
    }

    public ReqlAuthError(String message) {
        super(message);
    }

    public ReqlAuthError(String format, Object... args) {
        super(String.format(format, args));
    }

    public ReqlAuthError(String message, Throwable cause) {
        super(message, cause);
    }

    public ReqlAuthError(Throwable cause) {
        super(cause);
    }

    public ReqlAuthError(String msg, ReqlAst term, Backtrace bt) {
        super(msg);
        this.backtrace = bt;
        this.term = term;
    }

    public @Nullable Backtrace getBacktrace() {
        return backtrace;
    }

    public ReqlAuthError setBacktrace(Backtrace backtrace) {
        this.backtrace = backtrace;
        return this;
    }

    public @Nullable ReqlAst getTerm() {
        return this.term;
    }

    public ReqlAuthError setTerm(ReqlAst term) {
        this.term = term;
        return this;
    }
}
