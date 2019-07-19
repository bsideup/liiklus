package com.github.bsideup.liiklus.awsmsk.auth;

import lombok.SneakyThrows;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;

public class CertAndKeyParser {

    @SneakyThrows
    public PrivateKey toRSAPrivateKey(String privateKeyString) {
        var stringReader = new StringReader(privateKeyString);
        var pemObject = new PemReader(stringReader).readPemObject();
        byte[] pemContent = pemObject.getContent();
        var privateKeySpec = new PKCS8EncodedKeySpec(pemContent);
        var kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(privateKeySpec);
    }

    @SneakyThrows({IOException.class, CertificateException.class})
    public List<X509Certificate> toX509Certificates(String pemStringCertificates) {
        var pemParser = new PEMParser(new StringReader(pemStringCertificates));

        var certificates = new ArrayList<X509Certificate>();

        var currentObject = pemParser.readObject();
        while (currentObject != null) {
            if (currentObject instanceof X509CertificateHolder) {
                X509Certificate x509Cert = new JcaX509CertificateConverter().getCertificate((X509CertificateHolder) currentObject);
                certificates.add(x509Cert);
            } else {
                // be careful in logging pem, sometimes a pem can also contain private key, we should not just log it out
                throw new IllegalStateException("There are other non certificate objects inside the certificate chain");
            }

            currentObject = pemParser.readObject();
        }

        if (certificates.isEmpty()) {
            // be careful in logging pem, sometimes a pem can also contain private key, we should not just log it out
            throw new IllegalStateException("Certificate string does not contain any certificate");
        }

        return certificates;
    }

}
