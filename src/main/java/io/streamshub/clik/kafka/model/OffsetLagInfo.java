package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class OffsetLagInfo {
    private String topic;
    private int partition;
    private Long currentOffset;   // null if not committed yet
    private Long logEndOffset;
    private Long lag;             // null if currentOffset is null

    public OffsetLagInfo() {}

    public OffsetLagInfo(String topic, int partition, Long currentOffset, Long logEndOffset, Long lag) {
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = currentOffset;
        this.logEndOffset = logEndOffset;
        this.lag = lag;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(Long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public Long getLogEndOffset() {
        return logEndOffset;
    }

    public void setLogEndOffset(Long logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public Long getLag() {
        return lag;
    }

    public void setLag(Long lag) {
        this.lag = lag;
    }
}
