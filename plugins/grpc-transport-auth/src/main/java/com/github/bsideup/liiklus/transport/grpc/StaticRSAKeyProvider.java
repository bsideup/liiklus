package com.github.bsideup.liiklus.transport.grpc;

import com.auth0.jwt.interfaces.RSAKeyProvider;
import lombok.SneakyThrows;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class StaticRSAKeyProvider implements RSAKeyProvider {
    private Map<String, RSAPublicKey> keys;

    public StaticRSAKeyProvider(Map<String, String> keys) {
        this.keys = keys.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        key -> {
                            try {
                                return parsePubKey(key.getValue());
                            } catch (InvalidKeySpecException e) {
                                throw new IllegalArgumentException(String.format("Invalid RSA pubkey with id %s", key.getKey()), e);
                            }
                        }
                ));
    }

    @Override
    public RSAPublicKey getPublicKeyById(String keyId) {
        if (!keys.containsKey(keyId)) {
            throw new NoSuchElementException(String.format("KeyId %s is not defined to authorize GRPC requests", keyId));
        }
        return keys.get(keyId);
    }

    @Override
    public RSAPrivateKey getPrivateKey() {
        return null; // we don't sign anything
    }

    @Override
    public String getPrivateKeyId() {
        return null; // we don't sign anything
    }

    /**
     * Standard "ssh-rsa AAAAB3Nza..." pubkey representation could be converted to a proper format with
     * `ssh-keygen -f id_rsa.pub -e -m pkcs8`
     *
     * This method will work the same if you strip beginning, as well as line breaks on your own
     *
     * @param key X509 encoded (with -----BEGIN PUBLIC KEY----- lines)
     * @return parsed string
     */
    @SneakyThrows(NoSuchAlgorithmException.class)
    static RSAPublicKey parsePubKey(String key) throws InvalidKeySpecException {
        String keyContent = key.replaceAll("\\n", "")
                .replace("-----BEGIN PUBLIC KEY-----", "")
                .replace("-----END PUBLIC KEY-----", "");

        byte[] byteKey = Base64.getDecoder().decode(keyContent);
        var x509EncodedKeySpec = new X509EncodedKeySpec(byteKey);

        return (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(x509EncodedKeySpec);
    }
}
