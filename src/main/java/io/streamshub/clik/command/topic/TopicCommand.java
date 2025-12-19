package io.streamshub.clik.command.topic;

import picocli.CommandLine;

@CommandLine.Command(
        name = "topic",
        description = "Manage Kafka topics",
        subcommands = {
                CreateTopicCommand.class,
                ListTopicsCommand.class,
                DescribeTopicCommand.class,
                DeleteTopicCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class TopicCommand {
}
