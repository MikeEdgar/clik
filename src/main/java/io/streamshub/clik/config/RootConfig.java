package io.streamshub.clik.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class RootConfig {
    private String currentContext;
    private Map<String, String> settings = new HashMap<>();

    public RootConfig() {
    }

    public String getCurrentContext() {
        return currentContext;
    }

    public void setCurrentContext(String currentContext) {
        this.currentContext = currentContext;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings != null ? settings : new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RootConfig that = (RootConfig) o;
        return Objects.equals(currentContext, that.currentContext) &&
                Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentContext, settings);
    }

    @Override
    public String toString() {
        return "RootConfig{" +
                "currentContext='" + currentContext + '\'' +
                ", settings=" + settings +
                '}';
    }
}
