package com.github.bsideup.liiklus.config;

import org.springframework.context.annotation.Profile;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Profile(GatewayProfile.PROFILE_NAME)
public @interface GatewayProfile {

    String PROFILE_NAME = "gateway";
}
