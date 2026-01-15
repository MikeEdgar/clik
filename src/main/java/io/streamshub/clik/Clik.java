package io.streamshub.clik;

import io.streamshub.clik.command.acl.AclCommand;
import io.streamshub.clik.command.cluster.ClusterCommand;
import io.streamshub.clik.command.completion.CompletionCommand;
import io.streamshub.clik.command.consume.ConsumeCommand;
import io.streamshub.clik.command.context.ContextCommand;
import io.streamshub.clik.command.group.GroupCommand;
import io.streamshub.clik.command.produce.ProduceCommand;
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
                AclCommand.class,
                ClusterCommand.class,
                ContextCommand.class,
                TopicCommand.class,
                GroupCommand.class,
                ProduceCommand.class,
                ConsumeCommand.class,
                CommandLine.HelpCommand.class,
                CompletionCommand.class,
        }
)
public class Clik {
    public static final String NAME = "clik";

    Clik() {
        // No instances
    }

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
