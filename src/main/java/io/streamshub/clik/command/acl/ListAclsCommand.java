package io.streamshub.clik.command.acl;

import java.util.List;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBindingFilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.BaseCommand;
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
        name = "list",
        description = "List ACL bindings with optional filtering"
)
public class ListAclsCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

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
        try (Admin admin = clientFactory.createAdminClient()) {
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

            List<AclInfo> acls = aclService.listAcls(admin, filter);

            if (acls.isEmpty()) {
                out().println("No ACLs found.");
                return 0;
            }

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(acls);
                    break;
                case "yaml":
                    printYaml(acls);
                    break;
                case "json":
                    printJson(acls);
                    break;
                default:
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: table, yaml, json");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            err().println("Error: Invalid parameter: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to list ACLs: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(List<AclInfo> acls) {
        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, acls, List.of(
                column("PRINCIPAL", HorizontalAlign.LEFT, AclInfo::principal),
                column("RESOURCE TYPE", HorizontalAlign.LEFT, AclInfo::resourceType),
                column("RESOURCE NAME", HorizontalAlign.LEFT, AclInfo::resourceName),
                column("PATTERN", HorizontalAlign.LEFT, AclInfo::patternType),
                column("OPERATION", HorizontalAlign.LEFT, AclInfo::operation),
                column("PERMISSION", HorizontalAlign.LEFT, AclInfo::permissionType),
                column("HOST", HorizontalAlign.LEFT, AclInfo::host)
        ));

        out().println(table);
    }

    private void printYaml(List<AclInfo> acls) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().println(yamlMapper.writeValueAsString(acls));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(List<AclInfo> acls) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(acls));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
