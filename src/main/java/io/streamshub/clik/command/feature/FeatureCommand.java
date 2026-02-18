package io.streamshub.clik.command.feature;

import picocli.CommandLine;

@CommandLine.Command(
    name = "feature",
    description = "Manage Kafka cluster features",
    subcommands = {
        ListFeaturesCommand.class,
        DescribeFeatureCommand.class,
        AlterFeatureCommand.class,
        CommandLine.HelpCommand.class
    }
)
public class FeatureCommand {
    // Parent command - no implementation needed
}
