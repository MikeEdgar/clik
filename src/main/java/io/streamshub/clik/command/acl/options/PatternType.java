package io.streamshub.clik.command.acl.options;

import picocli.CommandLine;

public class PatternType {

    private PatternType() {
        // No instances
    }

    public static class FilterOption {
        static class Candidates extends EnumeratedOptionSupport {
            public Candidates() {
                super(without(org.apache.kafka.common.resource.PatternType.UNKNOWN));
            }
        }

        @CommandLine.Option(
                names = {"--pattern-type"},
                description = "Pattern type for filtering (default: ${DEFAULT-VALUE})",
                defaultValue = "LITERAL",
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
                        org.apache.kafka.common.resource.PatternType.ANY,
                        org.apache.kafka.common.resource.PatternType.MATCH,
                        org.apache.kafka.common.resource.PatternType.UNKNOWN
                ));
            }
        }

        @CommandLine.Option(
                names = {"--pattern-type"},
                description = "Pattern type: PREFIXED or LITERAL (default: ${DEFAULT-VALUE})",
                defaultValue = "LITERAL",
                converter = Candidates.class,
                completionCandidates = Candidates.class
        )
        String value;

        public String value() {
            return value;
        }
    }
}
