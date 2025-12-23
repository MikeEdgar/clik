package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public class GroupInfo {
    private String groupId;
    private String type;              // "consumer", "share", "stream"
    private String state;             // "Stable", "Dead", "Empty", etc.
    private String protocol;
    private int memberCount;
    private CoordinatorInfo coordinator;
    private List<GroupMemberInfo> members;      // null for list, populated for describe
    private List<OffsetLagInfo> offsets;        // null for list, populated for describe

    public GroupInfo() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getMemberCount() {
        return memberCount;
    }

    public void setMemberCount(int memberCount) {
        this.memberCount = memberCount;
    }

    public CoordinatorInfo getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(CoordinatorInfo coordinator) {
        this.coordinator = coordinator;
    }

    public List<GroupMemberInfo> getMembers() {
        return members;
    }

    public void setMembers(List<GroupMemberInfo> members) {
        this.members = members;
    }

    public List<OffsetLagInfo> getOffsets() {
        return offsets;
    }

    public void setOffsets(List<OffsetLagInfo> offsets) {
        this.offsets = offsets;
    }

    public static class Builder {
        private final GroupInfo groupInfo = new GroupInfo();

        public Builder groupId(String groupId) {
            groupInfo.groupId = groupId;
            return this;
        }

        public Builder type(String type) {
            groupInfo.type = type;
            return this;
        }

        public Builder state(String state) {
            groupInfo.state = state;
            return this;
        }

        public Builder protocol(String protocol) {
            groupInfo.protocol = protocol;
            return this;
        }

        public Builder memberCount(int memberCount) {
            groupInfo.memberCount = memberCount;
            return this;
        }

        public Builder coordinator(CoordinatorInfo coordinator) {
            groupInfo.coordinator = coordinator;
            return this;
        }

        public Builder members(List<GroupMemberInfo> members) {
            groupInfo.members = members;
            return this;
        }

        public Builder offsets(List<OffsetLagInfo> offsets) {
            groupInfo.offsets = offsets;
            return this;
        }

        public GroupInfo build() {
            return groupInfo;
        }
    }
}
