package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record CoordinatorInfo(int id, String host, int port) {}
