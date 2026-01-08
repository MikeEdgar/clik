package io.streamshub.clik.command.acl.options;

import org.apache.kafka.common.acl.AclPermissionType;

import picocli.CommandLine;

public class Permission {

    private Permission() {
        // No instances
    }

    public static class FilterOption {
        static class Candidates extends EnumeratedOptionSupport {
            public Candidates() {
                super(without(AclPermissionType.UNKNOWN));
            }
        }

        @CommandLine.Option(
                names = {"--permission"},
                description = "Filter by permission type",
                converter = Candidates.class,
                completionCandidates = Candidates.class
        )
        String value;

        public String value() {
            return value;
        }
    }

    public static class ValueOption {
        static class Candidates extends EnumeratedOptionSupport {
            public Candidates() {
                super(without(
                        AclPermissionType.ANY,
                        AclPermissionType.UNKNOWN
                ));
            }
        }

        @CommandLine.Option(
                names = {"--permission"},
                description = "Permission type: ALLOW or DENY (default: ${DEFAULT-VALUE})",
                defaultValue = "ALLOW",
                converter = Candidates.class,
                completionCandidates = Candidates.class
        )
        String value;

        public String value() {
            return value;
        }
    }
}
