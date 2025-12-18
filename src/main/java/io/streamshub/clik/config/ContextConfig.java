package io.streamshub.clik.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class ContextConfig {
    private Map<String, String> common = new HashMap<>();
    private Map<String, String> admin = new HashMap<>();
    private Map<String, String> consumer = new HashMap<>();
    private Map<String, String> producer = new HashMap<>();

    public ContextConfig() {
    }

    public Map<String, String> getCommon() {
        return common;
    }

    public void setCommon(Map<String, String> common) {
        this.common = common != null ? common : new HashMap<>();
    }

    public Map<String, String> getAdmin() {
        return admin;
    }

    public void setAdmin(Map<String, String> admin) {
        this.admin = admin != null ? admin : new HashMap<>();
    }

    public Map<String, String> getConsumer() {
        return consumer;
    }

    public void setConsumer(Map<String, String> consumer) {
        this.consumer = consumer != null ? consumer : new HashMap<>();
    }

    public Map<String, String> getProducer() {
        return producer;
    }

    public void setProducer(Map<String, String> producer) {
        this.producer = producer != null ? producer : new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextConfig that = (ContextConfig) o;
        return Objects.equals(common, that.common) &&
                Objects.equals(admin, that.admin) &&
                Objects.equals(consumer, that.consumer) &&
                Objects.equals(producer, that.producer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(common, admin, consumer, producer);
    }

    @Override
    public String toString() {
        return "ContextConfig{" +
                "common=" + common +
                ", admin=" + admin +
                ", consumer=" + consumer +
                ", producer=" + producer +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ContextConfig config = new ContextConfig();

        public Builder common(Map<String, String> common) {
            config.setCommon(common);
            return this;
        }

        public Builder admin(Map<String, String> admin) {
            config.setAdmin(admin);
            return this;
        }

        public Builder consumer(Map<String, String> consumer) {
            config.setConsumer(consumer);
            return this;
        }

        public Builder producer(Map<String, String> producer) {
            config.setProducer(producer);
            return this;
        }

        public Builder addCommon(String key, String value) {
            config.common.put(key, value);
            return this;
        }

        public Builder addAdmin(String key, String value) {
            config.admin.put(key, value);
            return this;
        }

        public Builder addConsumer(String key, String value) {
            config.consumer.put(key, value);
            return this;
        }

        public Builder addProducer(String key, String value) {
            config.producer.put(key, value);
            return this;
        }

        public ContextConfig build() {
            return config;
        }
    }
}
