package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record GroupInfo(
    String groupId,
    String type,              // "consumer", "share", "stream"
    String state,             // "Stable", "Dead", "Empty", etc.
    String protocol,
    int memberCount,
    CoordinatorInfo coordinator,
    List<GroupMemberInfo> members,      // null for list, populated for describe
    List<OffsetLagInfo> offsets        // null for list, populated for describe
) {
    // Compact constructor with defensive copying (preserve null semantics)
    public GroupInfo {
        members = members != null ? List.copyOf(members) : null;
        offsets = offsets != null ? List.copyOf(offsets) : null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String groupId;
        private String type;
        private String state;
        private String protocol;
        private int memberCount;
        private CoordinatorInfo coordinator;
        private List<GroupMemberInfo> members;
        private List<OffsetLagInfo> offsets;

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder protocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder memberCount(int memberCount) {
            this.memberCount = memberCount;
            return this;
        }

        public Builder coordinator(CoordinatorInfo coordinator) {
            this.coordinator = coordinator;
            return this;
        }

        public Builder members(List<GroupMemberInfo> members) {
            this.members = members;
            return this;
        }

        public Builder offsets(List<OffsetLagInfo> offsets) {
            this.offsets = offsets;
            return this;
        }

        public GroupInfo build() {
            return new GroupInfo(groupId, type, state, protocol,
                               memberCount, coordinator, members, offsets);
        }
    }
}
