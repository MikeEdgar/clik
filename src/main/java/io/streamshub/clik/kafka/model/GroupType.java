package io.streamshub.clik.kafka.model;

public enum GroupType {
    CONSUMER("consumer"),
    SHARE("share"),
    STREAM("stream");

    private final String name;

    GroupType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static GroupType fromString(String type) {
        for (GroupType gt : values()) {
            if (gt.name.equalsIgnoreCase(type)) {
                return gt;
            }
        }
        throw new IllegalArgumentException("Unknown group type: " + type);
    }
}
