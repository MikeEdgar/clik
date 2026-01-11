package io.streamshub.clik.command.cluster;

import picocli.CommandLine;

@CommandLine.Command(
        name = "cluster",
        description = "Manage Kafka cluster operations",
        subcommands = {
                DescribeClusterCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class ClusterCommand {
    // Parent command - no implementation needed
}
