package io.streamshub.clik.command.completion;

import picocli.CommandLine;

@CommandLine.Command(
        name = "completion",
        description = "Shell completion generator commands",
        mixinStandardHelpOptions = true,
        subcommands = {
                GenerateCompletionCommand.class,
                CandidatesCompletionCommand.class
        }
)
public class CompletionCommand {
}
