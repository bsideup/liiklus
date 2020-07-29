package com.github.bsideup.liiklus.transport.rsocket;

import io.rsocket.core.RSocketServer;

public interface RSocketServerConfigurer {

    void apply(RSocketServer server);
}
