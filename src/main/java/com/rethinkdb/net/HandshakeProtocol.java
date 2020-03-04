package com.rethinkdb.net;

import com.rethinkdb.gen.ast.Wait;
import com.rethinkdb.gen.exc.ReqlAuthError;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.proto.Protocol;
import com.rethinkdb.gen.proto.Version;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

import static com.rethinkdb.net.Crypto.*;
import static com.rethinkdb.net.Util.readJSON;

/**
 * Internal class used by {@link Connection#connect()} to do a proper handshake with the server.
 */
abstract class HandshakeProtocol {
    public static final Version VERSION = Version.V1_0;
    public static final Long SUB_PROTOCOL_VERSION = 0L;
    public static final Protocol PROTOCOL = Protocol.JSON;

    public static final String CLIENT_KEY = "Client Key";
    public static final String SERVER_KEY = "Server Key";

    static void doHandshake(ConnectionSocket socket, String username, String password, Long timeout) {
        // initialize handshake
        HandshakeProtocol handshake = new WaitingForProtocolRange(username, password);
        // Sit in the handshake until it's completed. Exceptions will be thrown if
        // anything goes wrong.
        while (!handshake.isFinished()) {
            ByteBuffer toWrite = handshake.toSend();
            if (toWrite != null) {
                socket.write(toWrite);
            }
            handshake = handshake.nextState(socket.readCString(timeout));
        }
    }

    private HandshakeProtocol() {
    }

    protected abstract HandshakeProtocol nextState(String response);

    @Nullable
    protected abstract ByteBuffer toSend();

    protected abstract boolean isFinished();

    private static void throwIfFailure(Map<String, Object> json) {
        if (!(boolean) json.get("success")) {
            Long errorCode = (Long) json.get("error_code");
            if (errorCode >= 10 && errorCode <= 20) {
                throw new ReqlAuthError((String) json.get("error"));
            } else {
                throw new ReqlDriverError((String) json.get("error"));
            }
        }
    }

    static class WaitingForProtocolRange extends HandshakeProtocol {
        private final String nonce;
        private final ByteBuffer message;
        private final ScramAttributes clientFirstMessageBare;
        private final byte[] password;

        WaitingForProtocolRange(String username, String password) {
            this.password = password.getBytes(StandardCharsets.UTF_8);
            this.nonce = makeNonce();

            // We could use a json serializer, but it's fairly straightforward
            this.clientFirstMessageBare = new ScramAttributes()
                .username(username)
                .nonce(nonce);
            byte[] jsonBytes = ("{" +
                "\"protocol_version\":" + SUB_PROTOCOL_VERSION + "," +
                "\"authentication_method\":\"SCRAM-SHA-256\"," +
                "\"authentication\":" + "\"n,," + clientFirstMessageBare + "\"" +
                "}").getBytes(StandardCharsets.UTF_8);
            // Creating the ByteBuffer over an underlying array makes
            // it easier to turn into a string later.
            //return ByteBuffer.wrap(new byte[capacity]).order(ByteOrder.LITTLE_ENDIAN);
            // size of VERSION
            // json auth payload
            // terminating null byte
            this.message = ByteBuffer.allocate(Integer.BYTES +    // size of VERSION
                jsonBytes.length + // json auth payload
                1).order(ByteOrder.LITTLE_ENDIAN).putInt(VERSION.value)
                .put(jsonBytes)
                .put(new byte[1]);
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Util.readJSON(response);
            throwIfFailure(json);
            long minVersion = (long) json.get("min_protocol_version");
            long maxVersion = (long) json.get("max_protocol_version");
            if (SUB_PROTOCOL_VERSION < minVersion || SUB_PROTOCOL_VERSION > maxVersion) {
                throw new ReqlDriverError(
                    "Unsupported protocol version " + SUB_PROTOCOL_VERSION +
                        ", expected between " + minVersion + " and " + maxVersion);
            }
            return new WaitingForAuthResponse(nonce, password, clientFirstMessageBare);
        }

        @Override
        public ByteBuffer toSend() {
            return message;
        }

        @Override
        public boolean isFinished() {
            return false;
        }
    }

    static class WaitingForAuthResponse extends HandshakeProtocol {
        private final String nonce;
        private final byte[] password;
        private final ScramAttributes clientFirstMessageBare;

        WaitingForAuthResponse(
            String nonce, byte[] password, ScramAttributes clientFirstMessageBare) {
            this.nonce = nonce;
            this.password = password;
            this.clientFirstMessageBare = clientFirstMessageBare;
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Util.readJSON(response);
            throwIfFailure(json);
            String serverFirstMessage = (String) json.get("authentication");
            ScramAttributes serverAuth = ScramAttributes.from(serverFirstMessage);
            if (!serverAuth.nonce().startsWith(nonce)) {
                throw new ReqlAuthError("Invalid nonce from server");
            }
            ScramAttributes clientFinalMessageWithoutProof = new ScramAttributes()
                .headerAndChannelBinding("biws")
                .nonce(serverAuth.nonce());

            // SaltedPassword := Hi(Normalize(password), salt, i)
            byte[] saltedPassword = pbkdf2(
                password, serverAuth.salt(), serverAuth.iterationCount());

            // ClientKey := HMAC(SaltedPassword, "Client Key")
            byte[] clientKey = hmac(saltedPassword, CLIENT_KEY);

            // StoredKey := H(ClientKey)
            byte[] storedKey = sha256(clientKey);

            // AuthMessage := client-first-message-bare + "," +
            //                server-first-message + "," +
            //                client-final-message-without-proof
            String authMessage =
                clientFirstMessageBare + "," +
                    serverFirstMessage + "," +
                    clientFinalMessageWithoutProof;

            // ClientSignature := HMAC(StoredKey, AuthMessage)
            byte[] clientSignature = hmac(storedKey, authMessage);

            // ClientProof := ClientKey XOR ClientSignature
            byte[] clientProof = xor(clientKey, clientSignature);

            // ServerKey := HMAC(SaltedPassword, "Server Key")
            byte[] serverKey = hmac(saltedPassword, SERVER_KEY);

            // ServerSignature := HMAC(ServerKey, AuthMessage)
            byte[] serverSignature = hmac(serverKey, authMessage);

            ScramAttributes auth = clientFinalMessageWithoutProof
                .clientProof(clientProof);
            byte[] authJson = ("{\"authentication\":\"" + auth + "\"}").getBytes(StandardCharsets.UTF_8);

            ByteBuffer message = ByteBuffer.allocate(authJson.length + 1).order(ByteOrder.LITTLE_ENDIAN)
                .put(authJson)
                .put(new byte[1]);
            return new WaitingForAuthSuccess(serverSignature, message);
        }

        @Override
        public ByteBuffer toSend() {
            return null;
        }

        @Override
        public boolean isFinished() {
            return false;
        }
    }

    static class HandshakeSuccess extends HandshakeProtocol {
        @Override
        public HandshakeProtocol nextState(String response) {
            return this;
        }

        @Override
        public ByteBuffer toSend() {
            return null;
        }

        @Override
        public boolean isFinished() {
            return true;
        }
    }

    static class WaitingForAuthSuccess extends HandshakeProtocol {
        private final byte[] serverSignature;
        private final ByteBuffer message;

        public WaitingForAuthSuccess(byte[] serverSignature, ByteBuffer message) {
            this.serverSignature = serverSignature;
            this.message = message;
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Util.readJSON(response);
            throwIfFailure(json);
            ScramAttributes auth = ScramAttributes
                .from((String) json.get("authentication"));
            if (!MessageDigest.isEqual(auth.serverSignature(), serverSignature)) {
                throw new ReqlAuthError("Invalid server signature");
            }
            return new HandshakeSuccess();
        }

        @Override
        public ByteBuffer toSend() {
            return message;
        }

        @Override
        public boolean isFinished() {
            return false;
        }
    }

    /**
     * Salted Challenge Response Authentication Mechanism (SCRAM) attributes
     */
    static class ScramAttributes {
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
        String authIdentity() {
            return _authIdentity;
        }

        String username() {
            return _username;
        }

        String nonce() {
            return _nonce;
        }

        String headerAndChannelBinding() {
            return _headerAndChannelBinding;
        }

        byte[] salt() {
            return _salt;
        }

        Integer iterationCount() {
            return _iterationCount;
        }

        String clientProof() {
            return _clientProof;
        }

        byte[] serverSignature() {
            return _serverSignature;
        }

        String error() {
            return _error;
        }
    }
}
