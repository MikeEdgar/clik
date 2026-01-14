package io.streamshub.clik.command;

import java.util.Optional;

import picocli.CommandLine;

public abstract class ContextualCommand extends BaseCommand {

    @CommandLine.Option(
            names = {"--context"},
            paramLabel = "contextName",
            description = "Use the given context (and not the set current context) for this command"
    )
    protected Optional<String> contextName;

}
