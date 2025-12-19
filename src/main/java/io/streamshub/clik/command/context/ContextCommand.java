package io.streamshub.clik.command.context;

import picocli.CommandLine;

@CommandLine.Command(
        name = "context",
        description = "Manage Kafka connection contexts",
        subcommands = {
                CreateContextCommand.class,
                ListContextsCommand.class,
                UseContextCommand.class,
                CurrentContextCommand.class,
                DeleteContextCommand.class,
                ShowContextCommand.class,
                RenameContextCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class ContextCommand {
}
