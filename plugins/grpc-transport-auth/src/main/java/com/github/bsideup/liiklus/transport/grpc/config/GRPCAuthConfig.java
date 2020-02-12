package com.github.bsideup.liiklus.transport.grpc.config;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.avast.grpc.jwt.server.JwtServerInterceptor;
import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusTransportConfigurer;
import com.github.bsideup.liiklus.transport.grpc.StaticRSAKeyProvider;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import io.grpc.netty.NettyServerBuilder;
import lombok.Data;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.group.GroupSequenceProvider;
import org.hibernate.validator.spi.group.DefaultGroupSequenceProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;

import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class GRPCAuthConfig implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        var authProperties = PropertiesUtil.bind(environment, new GRPCAuthProperties());

        if (authProperties.getAlg() == GRPCAuthProperties.Alg.NONE) {
            return;
        }

        log.info("GRPC Authorization ENABLED with algorithm {}", authProperties.getAlg());

        // Init it early to check that everything is fine in config
        JWTVerifier verifier = createVerifier(authProperties.getAlg(), authProperties);

        applicationContext.registerBean(
                JWTAuthGRPCTransportConfigurer.class,
                () -> new JWTAuthGRPCTransportConfigurer(verifier)
        );
    }

    private JWTVerifier createVerifier(GRPCAuthProperties.Alg alg, GRPCAuthProperties properties) {
        switch (alg) {
            case HMAC512:
                return JWT
                        .require(Algorithm.HMAC512(properties.getSecret()))
                        .acceptLeeway(2)
                        .build();
            case RSA512:
                return JWT
                        .require(Algorithm.RSA512(new StaticRSAKeyProvider(properties.getKeys())))
                        .acceptLeeway(2)
                        .build();
            default:
                throw new IllegalStateException("Unsupported algorithm");
        }
    }

    @Value
    static class JWTAuthGRPCTransportConfigurer implements GRPCLiiklusTransportConfigurer {
        private JWTVerifier verifier;

        @Override
        public void apply(NettyServerBuilder builder) {
            builder.intercept(new JwtServerInterceptor<>(verifier::verify));
        }
    }

    @ConfigurationProperties("grpc.auth")
    @Data
    @GroupSequenceProvider(GRPCAuthProperties.EnabledSequenceProvider.class)
    static class GRPCAuthProperties {

        Alg alg = Alg.NONE;

        @NotEmpty(groups = Symmetric.class)
        String secret;

        @NotEmpty(groups = Asymmetric.class)
        Map<String, String> keys = Map.of();

        enum Alg {
            NONE,
            RSA512,
            HMAC512,
        }

        interface Symmetric {
        }

        interface Asymmetric {
        }

        public static class EnabledSequenceProvider implements DefaultGroupSequenceProvider<GRPCAuthProperties> {

            @Override
            public List<Class<?>> getValidationGroups(GRPCAuthProperties object) {
                var sequence = new ArrayList<Class<?>>();
                sequence.add(GRPCAuthProperties.class);
                if (object != null && object.getAlg() == Alg.HMAC512) {
                    sequence.add(Symmetric.class);
                }
                if (object != null && object.getAlg() == Alg.RSA512) {
                    sequence.add(Asymmetric.class);
                }
                return sequence;
            }
        }
    }
}