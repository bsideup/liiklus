package com.github.bsideup.liiklus.transport.grpc;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticRSAKeyProviderTest {

    private static final String STRIPPED_4096 = "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEApkuZHC50VVyc6mkqWMXl" +
            "+fuhBmXhw8N8w6A0mlxRDHltdsPYvxE5n/Id4xUDCISZfjXIuSVyq/a7K3esbEqc" +
            "fs6/dm6PQuLMsaxEzS3Gxn+QxELJ41IyKiT0DFAhorSoFChfzkkS7whHm+O8wVDI" +
            "Z0Aj5TjY5t0/1CvU7wKeMDjVqOR3usEb37/5qu4ps0RbgQzBKjoJ3LSo/tt4tZw+" +
            "V3dT2lEVCKCA9OA0I5UXFUwUyMH8NudSlEpExGcmNHM4sEW4NK4Y7RW9tyDT0RQR" +
            "ydUIP8rXkjqyxMyHnwNUuzxJHqIXAdEhzw2xGLBSxr87wfmK09TEfSjmMemHfCfF" +
            "Ht+esDSy7zRB68hCS/chyN57xyBWG3BeaKeJm34gLU6gt+9Bhvq90a0RXA7TXK7y" +
            "QwhDQQwNPhUQshE036l/jCDxmgJZPNkvpweAeROsoEDf5o0TRaybXbyQh+jn+iJP" +
            "ve7K2bTixmjlQKOWB4HZ+1YWyTzUabpdeuHVokKuVFzpKqi5oid3Bz17XU4fN36e" +
            "M9CSV1urnlgdVwKwYttFwuerstwpB2rOT1UmamQhPwfDGy9x2d2vghSi+ELzKkKv" +
            "yAlkIdeK/WLIi3l/R4pCFC1JfAGagXS+Jtvr9+PkiD3bG220HpW1ry68CZcsO91z" +
            "7UCJcQMxXdt1gk3K+EbWaDUCAwEAAQ==";

    private static final String FULL_2048 = "-----BEGIN PUBLIC KEY-----\n" +
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6b/6nQLIQQ8fHT4PcSyb\n" +
            "hOLUE/237dgicbjsE7/Z/uPffuc36NTMJ122ppz6dWYnCrQ6CeTgAde4hlLE7Kvv\n" +
            "aFiUbe5XKwSL8KV292XqrwRZhMI58TTTygcrBodYGzHy0Yytv703rz+9Qt5HO5BF\n" +
            "02/+sM+Z0wlH6aXl3K3/2HfSOfitqnArBGaAs+PRNX2jlVKD1c9Cb7vo5L0X7q+6\n" +
            "55uBErEoN7IHbj1u33qI/xEvPSycIiT2RXMGZkvDZH6mTsALel4aP4Qpp1NcE+kD\n" +
            "itoBYAPTGgR4gBQveXZmD10yUVgJl2icINY3FvT9oJB6wgCY9+iTvufPppT1RPFH\n" +
            "dQIDAQAB\n" +
            "-----END PUBLIC KEY-----\n";

    @ParameterizedTest
    @ValueSource(strings = {
            STRIPPED_4096,
            FULL_2048
    })
    void shouldParseX509(String pubkey) throws InvalidKeySpecException {
        var parsed = StaticRSAKeyProvider.parsePubKey(pubkey);
        assertThat(parsed).isNotNull();
        assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
    }

    @Test
    void shouldThrowExceptionOnInvalid() {
        assertThatThrownBy(() -> StaticRSAKeyProvider.parsePubKey("")).isInstanceOf(InvalidKeySpecException.class);
    }

    @Test
    void shouldCreateProviderInstance() {
        new StaticRSAKeyProvider(Map.of(
                "valid", STRIPPED_4096
        ));
    }

    @Test
    void shouldHandleValidAndInvalidWithExceptionInConstructor() {
        assertThatThrownBy(() -> new StaticRSAKeyProvider(Map.of(
                "valid", STRIPPED_4096,
                "invalid", ""
        )))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldComplainOnNotFoundKey() {
        assertThatThrownBy(() -> new StaticRSAKeyProvider(Map.of(
                "valid", STRIPPED_4096
        )).getPublicKeyById("unknown"))
                .isInstanceOf(NoSuchElementException.class);
    }
}
