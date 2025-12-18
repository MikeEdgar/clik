package io.streamshub.clik;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine;

@TopCommand
@CommandLine.Command(name = Clik.NAME,
        mixinStandardHelpOptions = true,
        version = "${quarkus.application.version}",
        description = "Command line interface for Apache Kafka",
        subcommands = {
                CommandLine.HelpCommand.class,
        }
)
public class Clik {
    public static final String NAME = "clik";
}
