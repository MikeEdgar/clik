package io.streamshub.clik.command.acl.options;

import org.apache.kafka.common.acl.AclOperation;

import picocli.CommandLine;

public class Operation {

    private Operation() {
        // No instances
    }

    public static class FilterOption {
        static class Candidates extends EnumeratedOptionSupport {
            public Candidates() {
                super(without(AclOperation.UNKNOWN));
            }
        }

        @CommandLine.Option(
                names = {"--operation"},
                description = "Filter by operation",
                converter = Candidates.class,
                completionCandidates = Candidates.class
        )
        String operation;

        public String value() {
            return operation;
        }
    }

    public static class ValueOption {
        static class Candidates extends EnumeratedOptionSupport {
            public Candidates() {
                super(without(AclOperation.ANY, AclOperation.UNKNOWN));
            }
        }

        @CommandLine.Option(
                names = {"--operation", "-o"},
                required = true,
                description = "Operation type",
                converter = Candidates.class,
                completionCandidates = Candidates.class
        )
        String operation;

        public String value() {
            return operation;
        }
    }
}
