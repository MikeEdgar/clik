package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public class PartitionInfo {
    private int id;
    private int leader;
    private List<Integer> replicas;
    private List<Integer> isr;

    public PartitionInfo() {
    }

    public PartitionInfo(int id, int leader, List<Integer> replicas, List<Integer> isr) {
        this.id = id;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    public List<Integer> getIsr() {
        return isr;
    }

    public void setIsr(List<Integer> isr) {
        this.isr = isr;
    }
}
