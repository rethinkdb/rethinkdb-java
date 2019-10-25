package com.rethinkdb.net;

import com.rethinkdb.gen.exc.ReqlAuthError;
import org.jetbrains.annotations.Nullable;

public class ScramAttributes {
    @Nullable String _authIdentity; // a
    @Nullable String _username;     // n
    @Nullable String _nonce;        // r
    @Nullable String _headerAndChannelBinding; // c
    @Nullable byte[] _salt; // s
    @Nullable Integer _iterationCount; // i
    @Nullable String _clientProof; // p
    @Nullable byte[] _serverSignature; // v
    @Nullable String _error; // e
    @Nullable String _originalString;

    public static ScramAttributes create() {
        return new ScramAttributes();
    }

    static ScramAttributes from(ScramAttributes other) {
        ScramAttributes out = new ScramAttributes();
        out._authIdentity = other._authIdentity;
        out._username = other._username;
        out._nonce = other._nonce;
        out._headerAndChannelBinding = other._headerAndChannelBinding;
        out._salt = other._salt;
        out._iterationCount = other._iterationCount;
        out._clientProof = other._clientProof;
        out._serverSignature = other._serverSignature;
        out._error = other._error;
        return out;
    }

    static ScramAttributes from(String input) {
        ScramAttributes sa = new ScramAttributes();
        sa._originalString = input;
        for (String section : input.split(",")) {
            String[] keyVal = section.split("=", 2);
            sa.setAttribute(keyVal[0], keyVal[1]);
        }
        return sa;
    }

    private void setAttribute(String key, String val) {
        switch (key) {
            case "a":
                _authIdentity = val;
                break;
            case "n":
                _username = val;
                break;
            case "r":
                _nonce = val;
                break;
            case "m":
                throw new ReqlAuthError("m field disallowed");
            case "c":
                _headerAndChannelBinding = val;
                break;
            case "s":
                _salt = Crypto.fromBase64(val);
                break;
            case "i":
                _iterationCount = Integer.parseInt(val);
                break;
            case "p":
                _clientProof = val;
                break;
            case "v":
                _serverSignature = Crypto.fromBase64(val);
                break;
            case "e":
                _error = val;
                break;
            default:
                // Supposed to ignore unexpected fields
        }
    }

    public String toString() {
        if (_originalString != null) {
            return _originalString;
        }
        String output = "";
        if (_username != null) {
            output += ",n=" + _username;
        }
        if (_nonce != null) {
            output += ",r=" + _nonce;
        }
        if (_headerAndChannelBinding != null) {
            output += ",c=" + _headerAndChannelBinding;
        }
        if (_clientProof != null) {
            output += ",p=" + _clientProof;
        }
        if (output.startsWith(",")) {
            return output.substring(1);
        } else {
            return output;
        }
    }

    // Setters with coercion
    ScramAttributes username(String username) {
        ScramAttributes next = ScramAttributes.from(this);
        next._username = username.replace("=", "=3D").replace(",", "=2C");
        return next;
    }

    ScramAttributes nonce(String nonce) {
        ScramAttributes next = ScramAttributes.from(this);
        next._nonce = nonce;
        return next;
    }

    ScramAttributes headerAndChannelBinding(String hacb) {
        ScramAttributes next = ScramAttributes.from(this);
        next._headerAndChannelBinding = hacb;
        return next;
    }

    ScramAttributes clientProof(byte[] clientProof) {
        ScramAttributes next = ScramAttributes.from(this);
        next._clientProof = Crypto.toBase64(clientProof);
        return next;
    }

    // Getters
    String authIdentity() { return _authIdentity; }

    String username() { return _username; }

    String nonce() { return _nonce; }

    String headerAndChannelBinding() { return _headerAndChannelBinding; }

    byte[] salt() { return _salt; }

    Integer iterationCount() { return _iterationCount; }

    String clientProof() { return _clientProof; }

    byte[] serverSignature() { return _serverSignature; }

    String error() { return _error; }
}
