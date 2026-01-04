package io.streamshub.clik.config;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
record RootConfig(
    String currentContext,
    Map<String, String> settings
) {

    public RootConfig {
        settings = settings != null ? settings : new HashMap<>();
    }

    public RootConfig(String currentContext) {
        this(currentContext, null);
    }

    public RootConfig withCurrentContext(String currentContext) {
        return new RootConfig(currentContext, settings);
    }
}
