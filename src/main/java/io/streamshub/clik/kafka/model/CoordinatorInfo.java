package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class CoordinatorInfo {
    private int id;
    private String host;
    private int port;

    public CoordinatorInfo() {}

    public CoordinatorInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
