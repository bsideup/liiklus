package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusTransportConfigurer;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;

import java.io.File;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class GRPCTLSConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        var tlsProperties = PropertiesUtil.bind(environment, new GRPCTLSProperties());

        if (tlsProperties.getKey() == null) {
            return;
        }

        log.info("GRPC {}TLS ENABLED", tlsProperties.getTrustCert() != null ? "mutual " : "");

        applicationContext.registerBean(
                TLSGRPCTransportConfigurer.class,
                () -> new TLSGRPCTransportConfigurer(tlsProperties)
        );
    }

    @Value
    static class TLSGRPCTransportConfigurer implements GRPCLiiklusTransportConfigurer {

        GRPCTLSProperties properties;

        @Override
        public void apply(NettyServerBuilder builder) {
            SslContext ctx = createSSLContext(
                    properties.getKey(),
                    properties.getKeyPassword(),
                    properties.getKeyCertChain(),
                    properties.getTrustCert()
            );

            builder.sslContext(ctx);
        }

        /**
         * Mostly copy of the https://github.com/grpc/grpc-java/tree/master/examples/example-tls
         * and https://github.com/grpc/grpc-java/blob/master/SECURITY.md
         *
         * Refer to {@link io.netty.handler.ssl.SslContextBuilder#forServer(File keyCertChainFile, File keyFile, String keyPassword)}
         * for more details.
         *
         * @param key          a PKCS#8 private key file in PEM format
         * @param keyPassword  the password of the key or null if not protected
         * @param keyCertChain an X.509 certificate chain file in PEM format
         * @param trustCert    file should contain an X.509 certificate collection in PEM format
         * @return ready-to-use ssl context.
         */
        @SneakyThrows
        SslContext createSSLContext(Resource key, String keyPassword, Resource keyCertChain, Resource trustCert) {
            SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(
                    keyCertChain.getInputStream(),
                    key.getInputStream(),
                    keyPassword
            );
            if (trustCert != null) {
                sslClientContextBuilder.trustManager(trustCert.getInputStream());
                sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
            }
            return GrpcSslContexts.configure(sslClientContextBuilder).build();
        }
    }

    @ConfigurationProperties("grpc.tls")
    @Data
    static class GRPCTLSProperties {

        Resource key;

        String keyPassword;

        Resource keyCertChain;

        Resource trustCert;

    }
}