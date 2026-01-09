package io.streamshub.clik.command.acl;

import java.util.Collections;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.command.acl.options.Operation;
import io.streamshub.clik.command.acl.options.PatternType;
import io.streamshub.clik.command.acl.options.Permission;
import io.streamshub.clik.command.acl.options.Resource;
import io.streamshub.clik.kafka.AclService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;

@CommandLine.Command(
        name = "create",
        description = "Create new ACL bindings"
)
public class CreateAclCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"--principal", "-p"},
            required = true,
            description = "Principal in format: User:<username> or User:* for all users"
    )
    String principal;

    @Mixin
    Operation.ValueOption operationOption;

    @Mixin
    Permission.ValueOption permissionOption;

    @CommandLine.Option(
            names = {"--host"},
            description = "Host from which principal can access (default: ${DEFAULT-VALUE})",
            defaultValue = "*"
    )
    String host;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
    Resource.Options resource;

    @Mixin
    PatternType.ValueOption patternTypeOption;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    AclService aclService;

    @Override
    public Integer call() {
        // Validate principal format
        if (!principal.startsWith("User:")) {
            err().println("Error: Invalid principal format. Must be 'User:<username>' or 'User:*'");
            return 1;
        }

        var resourceSpec = Resource.fromOptions(resource);

        if (resourceSpec.isMissing()) {
            err().println("Error: Resource must be specified");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient()) {
            AclBinding binding = aclService.buildAclBinding(
                    resourceSpec.type(),
                    resourceSpec.name(),
                    patternTypeOption.value(),
                    principal,
                    host,
                    operationOption.value(),
                    permissionOption.value()
            );

            aclService.createAcls(admin, Collections.singleton(binding));

            out().println("ACL created successfully.");
            out().println("  Principal: " + principal);
            out().println("  Resource: " + resourceSpec.type() + ":" + resourceSpec.name() + " (" + patternTypeOption.value() + ")");
            out().println("  Operation: " + operationOption.value());
            out().println("  Permission: " + permissionOption.value());
            out().println("  Host: " + host);

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            err().println("Error: Invalid parameter: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to create ACL: " + e.getMessage());
            return 1;
        }
    }
}
