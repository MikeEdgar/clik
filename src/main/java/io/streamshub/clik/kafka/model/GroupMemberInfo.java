package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public class GroupMemberInfo {
    private String memberId;
    private String clientId;
    private String host;
    private List<PartitionAssignment> assignments;

    public GroupMemberInfo() {}

    public GroupMemberInfo(String memberId, String clientId, String host, List<PartitionAssignment> assignments) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.host = host;
        this.assignments = assignments;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public List<PartitionAssignment> getAssignments() {
        return assignments;
    }

    public void setAssignments(List<PartitionAssignment> assignments) {
        this.assignments = assignments;
    }

    @RegisterForReflection
    public static class PartitionAssignment {
        private String topic;
        private List<Integer> partitions;

        public PartitionAssignment() {}

        public PartitionAssignment(String topic, List<Integer> partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public List<Integer> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<Integer> partitions) {
            this.partitions = partitions;
        }
    }
}
