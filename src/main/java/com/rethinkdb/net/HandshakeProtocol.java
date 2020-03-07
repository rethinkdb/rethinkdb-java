package com.rethinkdb.net;

import com.rethinkdb.gen.exc.ReqlAuthError;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.proto.Protocol;
import com.rethinkdb.gen.proto.Version;
import com.rethinkdb.utils.Internals;
import org.jetbrains.annotations.Nullable;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal class used by {@link Connection#connect()} to do a proper handshake with the server.
 */
class HandshakeProtocol {
    private static final HandshakeProtocol FINISHED = new HandshakeProtocol();

    public static final Version VERSION = Version.V1_0;
    public static final Long SUB_PROTOCOL_VERSION = 0L;
    public static final Protocol PROTOCOL = Protocol.JSON;

    public static final String CLIENT_KEY = "Client Key";
    public static final String SERVER_KEY = "Server Key";

    private HandshakeProtocol() {
    }

    static void doHandshake(ConnectionSocket socket, String username, String password, Long timeout) {
        // initialize handshake
        HandshakeProtocol handshake = new WaitingForProtocolRange(username, password);
        // Sit in the handshake until it's completed. Exceptions will be thrown if
        // anything goes wrong.
        while (handshake != FINISHED) {
            ByteBuffer toWrite = handshake.toSend();
            if (toWrite != null) {
                socket.write(toWrite);
            }
            handshake = handshake.nextState(socket.readCString(timeout));
        }
    }

    @Nullable
    protected ByteBuffer toSend() {
        throw new IllegalStateException();
    }

    protected HandshakeProtocol nextState(String response) {
        throw new IllegalStateException();
    }

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
        private static final SecureRandom secureRandom = new SecureRandom();
        private static final int NONCE_BYTES = 18;

        private final String nonce;
        private final ScramAttributes clientFirstMessageBare;
        private final byte[] password;

        WaitingForProtocolRange(String username, String password) {
            this.password = password.getBytes(StandardCharsets.UTF_8);
            this.nonce = makeNonce();
            this.clientFirstMessageBare = new ScramAttributes()
                .username(username)
                .nonce(nonce);
        }

        @Override
        public ByteBuffer toSend() {
            byte[] jsonBytes = ("{" +
                "\"protocol_version\":" + SUB_PROTOCOL_VERSION + "," +
                "\"authentication_method\":\"SCRAM-SHA-256\"," +
                "\"authentication\":" + "\"n,," + clientFirstMessageBare + "\"" +
                "}").getBytes(StandardCharsets.UTF_8);
            // Creating the ByteBuffer over an underlying array makes
            // it easier to turn into a string later.
            //return ByteBuffer.wrap(new byte[capacity]).order(ByteOrder.LITTLE_ENDIAN);
            // size of VERSION + json auth payload + terminating null byte
            return ByteBuffer.allocate(Integer.BYTES + jsonBytes.length + 1)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(VERSION.value)
                .put(jsonBytes)
                .put(new byte[1]);
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Internals.readJson(response);
            throwIfFailure(json);
            long minVersion = (long) json.get("min_protocol_version");
            long maxVersion = (long) json.get("max_protocol_version");
            if (SUB_PROTOCOL_VERSION < minVersion || SUB_PROTOCOL_VERSION > maxVersion) {
                throw new ReqlDriverError("Unsupported protocol version " + SUB_PROTOCOL_VERSION + ", expected between " + minVersion + " and " + maxVersion);
            }
            return new WaitingForAuthResponse(nonce, password, clientFirstMessageBare);
        }

        static String makeNonce() {
            byte[] rawNonce = new byte[NONCE_BYTES];
            secureRandom.nextBytes(rawNonce);
            return Base64.getEncoder().encodeToString(rawNonce);
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
        public ByteBuffer toSend() {
            return null;
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Internals.readJson(response);
            throwIfFailure(json);
            ScramAttributes serverScram = ScramAttributes.from((String) json.get("authentication"));
            if (!Objects.requireNonNull(serverScram._nonce).startsWith(nonce)) {
                throw new ReqlAuthError("Invalid nonce from server");
            }
            ScramAttributes clientScram = new ScramAttributes()
                .headerAndChannelBinding("biws")
                .nonce(serverScram._nonce);

            // SaltedPassword := Hi(Normalize(password), salt, i)
            // ClientKey := HMAC(SaltedPassword, "Client Key")
            // StoredKey := H(ClientKey)
            byte[] saltedPassword = PBKDF2.compute(password, serverScram._salt, serverScram._iterationCount);
            byte[] clientKey = hmac(saltedPassword, CLIENT_KEY);
            byte[] storedKey = sha256(clientKey);

            // AuthMessage := client-first-message-bare + "," +
            //                server-first-message + "," +
            //                client-final-message-without-proof
            String authMessage = clientFirstMessageBare + "," + serverScram + "," + clientScram;

            // ClientSignature := HMAC(StoredKey, AuthMessage)
            // ClientProof := ClientKey XOR ClientSignature
            // ServerKey := HMAC(SaltedPassword, "Server Key")
            // ServerSignature := HMAC(ServerKey, AuthMessage)
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] clientProof = xor(clientKey, clientSignature);
            byte[] serverKey = hmac(saltedPassword, SERVER_KEY);
            byte[] serverSignature = hmac(serverKey, authMessage);

            return new WaitingForAuthSuccess(serverSignature, clientScram.clientProof(clientProof));
        }

        static byte[] sha256(byte[] clientKey) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(clientKey);
            } catch (NoSuchAlgorithmException e) {
                throw new ReqlDriverError(e);
            }
        }

        static byte[] hmac(byte[] key, String string) {
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(key, "HmacSHA256"));
                return mac.doFinal(string.getBytes(StandardCharsets.UTF_8));
            } catch (InvalidKeyException | NoSuchAlgorithmException e) {
                throw new ReqlDriverError(e);
            }
        }

        static byte[] xor(byte[] a, byte[] b) {
            if (a.length != b.length) {
                throw new ReqlDriverError("arrays must be the same length");
            }
            byte[] result = new byte[a.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) (a[i] ^ b[i]);
            }
            return result;
        }

        private static class PBKDF2 {
            static byte[] compute(byte[] password, byte[] salt, Integer iterationCount) {
                return cache.computeIfAbsent(new PBKDF2(password, salt, iterationCount), PBKDF2::compute);
            }

            private static final Map<PBKDF2, byte[]> cache = new ConcurrentHashMap<>();

            final byte[] password;
            final byte[] salt;
            final int iterations;

            PBKDF2(byte[] password, byte[] salt, int iterations) {
                this.password = password;
                this.salt = salt;
                this.iterations = iterations;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                PBKDF2 that = (PBKDF2) o;

                if (iterations != that.iterations) return false;
                if (!Arrays.equals(password, that.password)) return false;
                return Arrays.equals(salt, that.salt);
            }

            @Override
            public int hashCode() {
                int result = Arrays.hashCode(password);
                result = 31 * result + Arrays.hashCode(salt);
                result = 31 * result + iterations;
                return result;
            }

            public byte[] compute() {
                final PBEKeySpec spec = new PBEKeySpec(
                    new String(password, StandardCharsets.UTF_8).toCharArray(),
                    salt, iterations, 256
                );
                try {
                    return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
                } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                    throw new ReqlDriverError(e);
                }
            }
        }
    }

    static class WaitingForAuthSuccess extends HandshakeProtocol {
        private final byte[] serverSignature;
        private final ScramAttributes auth;

        public WaitingForAuthSuccess(byte[] serverSignature, ScramAttributes auth) {
            this.serverSignature = serverSignature;
            this.auth = auth;
        }

        @Override
        public ByteBuffer toSend() {
            byte[] authJson = ("{\"authentication\":\"" + auth + "\"}").getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(authJson.length + 1).order(ByteOrder.LITTLE_ENDIAN)
                .put(authJson)
                .put(new byte[1]);
        }

        @Override
        public HandshakeProtocol nextState(String response) {
            Map<String, Object> json = Internals.readJson(response);
            throwIfFailure(json);
            ScramAttributes auth = ScramAttributes.from((String) json.get("authentication"));
            if (!MessageDigest.isEqual(auth._serverSignature, serverSignature)) {
                throw new ReqlAuthError("Invalid server signature");
            }
            return FINISHED;
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
                    _salt = Base64.getDecoder().decode(val);
                    break;
                case "i":
                    _iterationCount = Integer.parseInt(val);
                    break;
                case "p":
                    _clientProof = val;
                    break;
                case "v":
                    _serverSignature = Base64.getDecoder().decode(val);
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
            StringJoiner j = new StringJoiner(",");
            if (_username != null) {
                j.add("n=" + _username);
            }
            if (_nonce != null) {
                j.add("r=" + _nonce);
            }
            if (_headerAndChannelBinding != null) {
                j.add("c=" + _headerAndChannelBinding);
            }
            if (_clientProof != null) {
                j.add("p=" + _clientProof);
            }
            return j.toString();
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
            next._clientProof = Base64.getEncoder().encodeToString(clientProof);
            return next;
        }
    }
}
