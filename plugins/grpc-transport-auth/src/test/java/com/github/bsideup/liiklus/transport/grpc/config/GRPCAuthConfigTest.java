package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.transport.grpc.config.GRPCAuthConfig.JWTAuthGRPCTransportConfigurer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class GRPCAuthConfigTest {
    private static final String FULL_2048 = "-----BEGIN PUBLIC KEY-----\n" +
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6b/6nQLIQQ8fHT4PcSyb\n" +
            "hOLUE/237dgicbjsE7/Z/uPffuc36NTMJ122ppz6dWYnCrQ6CeTgAde4hlLE7Kvv\n" +
            "aFiUbe5XKwSL8KV292XqrwRZhMI58TTTygcrBodYGzHy0Yytv703rz+9Qt5HO5BF\n" +
            "02/+sM+Z0wlH6aXl3K3/2HfSOfitqnArBGaAs+PRNX2jlVKD1c9Cb7vo5L0X7q+6\n" +
            "55uBErEoN7IHbj1u33qI/xEvPSycIiT2RXMGZkvDZH6mTsALel4aP4Qpp1NcE+kD\n" +
            "itoBYAPTGgR4gBQveXZmD10yUVgJl2icINY3FvT9oJB6wgCY9+iTvufPppT1RPFH\n" +
            "dQIDAQAB\n" +
            "-----END PUBLIC KEY-----\n";

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new GRPCAuthConfig());

    @Test
    void shouldRequireAlg() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "grpc.auth.alg: NONE"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(JWTAuthGRPCTransportConfigurer.class);
        });
    }

    @Test
    void shouldAddWithHmac512() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "grpc.auth.alg: HMAC512",
                "grpc.auth.secret: secret"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .hasSingleBean(JWTAuthGRPCTransportConfigurer.class);
        });
    }

    @Test
    void shouldAddWithRsa512() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "grpc.auth.alg: RSA512",
                "grpc.auth.keys.key: " + FULL_2048
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .hasSingleBean(JWTAuthGRPCTransportConfigurer.class);
        });
    }

    @Test
    void shouldValidateParametersHmac512() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "grpc.auth.alg: HMAC512"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
    }

    @Test
    void shouldValidateParametersRsa512() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "grpc.auth.alg: RSA512"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
    }
}