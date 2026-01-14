package io.streamshub.clik.command.acl;

import java.util.Collection;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBindingFilter;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.command.acl.options.Operation;
import io.streamshub.clik.command.acl.options.PatternType;
import io.streamshub.clik.command.acl.options.Permission;
import io.streamshub.clik.command.acl.options.Resource;
import io.streamshub.clik.kafka.AclService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.AclInfo;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;

@CommandLine.Command(
        name = "delete",
        description = "Delete ACL bindings matching filter"
)
public class DeleteAclCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-y", "--yes"},
            description = "Automatically confirm deletion without prompting"
    )
    boolean autoConfirm;

    @CommandLine.Option(
            names = {"--principal"},
            description = "Filter by principal (e.g., User:alice)"
    )
    String principal;

    @CommandLine.Option(
            names = {"--host"},
            description = "Filter by host"
    )
    String host;

    @Mixin
    Operation.FilterOption operationOption;

    @Mixin
    Permission.FilterOption permissionOption;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
    Resource.Options resource;

    @Mixin
    PatternType.FilterOption patternTypeOption;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    AclService aclService;

    @Override
    public Integer call() {
        // Validate that at least one filter is specified
        if (filtersUnspecified()) {
            err().println("Error: At least one filter must be specified for delete operation.");
            err().println();
            err().println("Available filters:");
            err().println("  --principal <user>       Filter by principal");
            err().println("  --topic <name>           Filter by topic");
            err().println("  --group <name>           Filter by consumer group");
            err().println("  --cluster                Filter by cluster");
            err().println("  --operation <op>         Filter by operation");
            err().println("  --permission <type>      Filter by permission type");
            err().println("  --host <host>            Filter by host");
            err().println("  --pattern-type <type>    Filter by pattern type");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            var resourceSpec = Resource.fromOptions(resource);

            AclBindingFilter filter = aclService.buildAclBindingFilter(
                    resourceSpec.type(),
                    resourceSpec.name(),
                    patternTypeOption.value(),
                    principal,
                    host,
                    operationOption.value(),
                    permissionOption.value()
            );

            // Preview matching ACLs
            Collection<AclInfo> matchingAcls = aclService.listAcls(admin, filter);

            if (matchingAcls.isEmpty()) {
                err().println("Error: No ACLs matched the specified criteria.");
                return 1;
            }

            // Show preview and confirm
            if (!autoConfirm) {
                out().println("The following ACL bindings will be deleted:");
                for (AclInfo acl : matchingAcls) {
                    out().println("  " + acl.principal() + " on " +
                            acl.resourceType() + ":" + acl.resourceName() +
                            " (" + acl.operation() + ", " + acl.permissionType() + ")");
                }
                out().println();

                String aclCount = matchingAcls.size() == 1
                        ? "1 ACL binding"
                        : matchingAcls.size() + " ACL bindings";
                out().print("Delete " + aclCount + "? This cannot be undone. [y/N]: ");

                String response;
                try (Scanner scanner = new Scanner(System.in)) {
                    response = scanner.nextLine().trim().toLowerCase();
                }

                if (!response.equals("y") && !response.equals("yes")) {
                    out().println("Delete cancelled.");
                    return 0;
                }
            }

            // Only delete the ACLs found initially and that the user confirmed.
            Collection<AclBindingFilter> deleteFilters = matchingAcls.stream()
                    .map(info -> aclService.buildAclBindingFilter(
                                info.resourceType(),
                                info.resourceName(),
                                info.patternType(),
                                info.principal(),
                                info.host(),
                                info.operation(),
                                info.permissionType()))
                    .toList();

            // Delete ACLs
            Collection<AclInfo> deleted = aclService.deleteAcls(admin, deleteFilters);

            if (deleted.size() == 1) {
                out().println("1 ACL binding deleted.");
            } else {
                out().println(deleted.size() + " ACL bindings deleted.");
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            err().println("Error: Invalid parameter: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to delete ACLs: " + e.getMessage());
            return 1;
        }
    }

    private boolean filtersUnspecified() {
        return Stream.of(principal, host, operationOption.value(), permissionOption.value())
                .allMatch(Objects::isNull)
            && Resource.fromOptions(resource).isMissing();
    }
}
