package io.streamshub.clik.command.acl;

import picocli.CommandLine;

@CommandLine.Command(
        name = "acl",
        description = "Manage Kafka Access Control Lists (ACLs)",
        subcommands = {
                CreateAclCommand.class,
                ListAclsCommand.class,
                DeleteAclCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class AclCommand {
}
