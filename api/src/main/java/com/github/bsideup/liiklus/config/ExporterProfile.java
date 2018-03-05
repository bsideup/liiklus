package com.github.bsideup.liiklus.config;

import org.springframework.context.annotation.Profile;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Profile(ExporterProfile.PROFILE_NAME)
public @interface ExporterProfile {

    String PROFILE_NAME = "exporter";
}
