package com.github.bsideup.liiklus.records;

import io.cloudevents.Attributes;

import java.time.ZonedDateTime;

public interface LiiklusAttributes extends Attributes {

    ZonedDateTime getTime();

}
