package io.streamshub.clik;

import io.streamshub.clik.command.context.ContextCommand;
import io.streamshub.clik.command.group.GroupCommand;
import io.streamshub.clik.command.topic.TopicCommand;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;

@TopCommand
@CommandLine.Command(name = Clik.NAME,
        mixinStandardHelpOptions = true,
        versionProvider = Clik.Version.class,
        description = "Command line interface for Apache Kafka",
        subcommands = {
                ContextCommand.class,
                TopicCommand.class,
                GroupCommand.class,
                CommandLine.HelpCommand.class,
        }
)
public class Clik {
    public static final String NAME = "clik";

    @Singleton
    public static class Version implements IVersionProvider {
        @ConfigProperty(name = "quarkus.application.version")
        String applicationVersion;

        @Override
        public String[] getVersion() throws Exception {
            return new String[] { applicationVersion };
        }
    }
}
