package io.streamshub.clik.command.acl.options;

import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

public record Resource(String type, String name) {
    public static final Resource UNSPECIFIED = new Resource(null, null);

    public static Resource fromOptions(Options options) {
        return options != null ? options.resource() : Resource.UNSPECIFIED;
    }

    public boolean isMissing() {
        return type == null && name == null;
    }

    public static class Options {
        @CommandLine.Option(
                names = {"--topic"},
                description = "Topic name (supports literal or prefix with --prefix)",
                completionCandidates = NameCandidate.Topic.class
        )
        String topic;

        @CommandLine.Option(
                names = {"--group"},
                description = "Consumer group name (supports literal or prefix with --prefix)",
                completionCandidates = NameCandidate.Group.class
        )
        String group;

        @CommandLine.Option(
                names = {"--cluster"},
                description = "Cluster resource (value ignored, use any non-empty string)",
                arity="0..1",
                fallbackValue = "kafka-cluster"
        )
        String cluster;

        @CommandLine.Option(
                names = {"--transactional-id"},
                description = "Transactional ID (supports literal or prefix with --prefix)"
        )
        String transactionalId;

        @CommandLine.Option(
                names = {"--delegation-token"},
                description = "Delegation token ID (supports literal or prefix with --prefix)"
        )
        String delegationToken;

        @CommandLine.Option(
                names = {"--user-resource"},
                description = "User resource (supports literal or prefix with --prefix)"
        )
        String userResource;

        public Resource resource() {
            if (topic != null) {
                return new Resource("TOPIC", topic);
            } else if (group != null) {
                return new Resource("GROUP", group);
            } else if (cluster != null) {
                return new Resource("CLUSTER", "kafka-cluster");
            } else if (transactionalId != null) {
                return new Resource("TRANSACTIONAL_ID", transactionalId);
            } else if (delegationToken != null) {
                return new Resource("DELEGATION_TOKEN", delegationToken);
            } else if (userResource != null) {
                return new Resource("USER", userResource);
            } else {
                return Resource.UNSPECIFIED;
            }
        }
    }
}
