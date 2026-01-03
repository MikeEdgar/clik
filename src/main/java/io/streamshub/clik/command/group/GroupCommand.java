package io.streamshub.clik.command.group;

import picocli.CommandLine;

@CommandLine.Command(
        name = "group",
        description = "Manage Kafka groups (consumer, share, stream)",
        subcommands = {
                ListGroupsCommand.class,
                DescribeGroupCommand.class,
                DeleteGroupCommand.class,
                AlterGroupCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class GroupCommand {
}
