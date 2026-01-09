package io.streamshub.clik.command.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.GroupInfo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "list",
        description = "List all Kafka consumer groups"
)
public class ListGroupsCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json, name (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @CommandLine.Option(
            names = {"--type"},
            description = "Filter by group type: consumer, share, stream"
    )
    String typeFilter;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    GroupService groupService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient()) {
            Collection<GroupInfo> groups = groupService.listGroups(admin, typeFilter);

            if (groups.isEmpty()) {
                out().println("No groups found.");
                return 0;
            }

            // Sort groups by name for consistent output order
            List<GroupInfo> sortedGroups = new ArrayList<>(groups);
            sortedGroups.sort((a, b) -> a.groupId().compareTo(b.groupId()));

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(sortedGroups);
                    break;
                case "name":
                    printNames(sortedGroups);
                    break;
                case "yaml":
                    printYaml(sortedGroups);
                    break;
                case "json":
                    printJson(sortedGroups);
                    break;
                default:
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: table, yaml, json, name");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to list groups: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(List<GroupInfo> groups) {
        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, groups, List.of(
                column("NAME", HorizontalAlign.LEFT, GroupInfo::groupId),
                column("TYPE", HorizontalAlign.LEFT, GroupInfo::type),
                column("PROTOCOL", HorizontalAlign.LEFT, group -> {
                    var protocol = group.protocol();
                    return protocol.isEmpty() ? "-" : protocol;
                }),
                column("STATE", HorizontalAlign.LEFT, GroupInfo::state),
                column("MEMBERS", HorizontalAlign.LEFT, group -> {
                    if (group.describeError() != null) {
                        return "Not Available";
                    }
                    return String.valueOf(group.memberCount());
                })
        ));

        out().println(table);
    }

    private static <T> ColumnData<T> column(String name, HorizontalAlign dataAlign, Function<T, String> data) {
        return new Column().header(name).headerAlign(HorizontalAlign.LEFT).dataAlign(dataAlign).with(data);
    }

    private void printNames(List<GroupInfo> groups) {
        groups.forEach(group -> out().println(group.groupId()));
    }

    private void printYaml(List<GroupInfo> groups) {
        try {
            List<Map<String, Object>> groupList = new ArrayList<>();

            for (GroupInfo group : groups) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("groupId", group.groupId());
                data.put("type", group.type());
                data.put("state", group.state());
                data.put("members", group.memberCount());
                groupList.add(data);
            }

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().println(yamlMapper.writeValueAsString(groupList));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(List<GroupInfo> groups) {
        try {
            List<Map<String, Object>> groupList = new ArrayList<>();

            for (GroupInfo group : groups) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("groupId", group.groupId());
                data.put("type", group.type());
                data.put("state", group.state());
                data.put("members", group.memberCount());
                groupList.add(data);
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(groupList));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
