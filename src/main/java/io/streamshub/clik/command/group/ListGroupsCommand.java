package io.streamshub.clik.command.group;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.GroupInfo;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "list",
        description = "List all Kafka consumer groups"
)
public class ListGroupsCommand implements Callable<Integer> {

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
                System.out.println("No groups found.");
                return 0;
            }

            // Sort groups by name for consistent output order
            List<GroupInfo> sortedGroups = new ArrayList<>(groups);
            sortedGroups.sort((a, b) -> a.getGroupId().compareTo(b.getGroupId()));

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
                    System.err.println("Error: Unknown output format: " + outputFormat);
                    System.err.println("Valid formats: table, yaml, json, name");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to list groups: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(List<GroupInfo> groups) {
        List<GroupRow> rows = new ArrayList<>();

        for (GroupInfo group : groups) {
            rows.add(new GroupRow(
                    group.getGroupId(),
                    group.getType(),
                    group.getState(),
                    String.valueOf(group.getMemberCount())
            ));
        }

        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                new Column().header("NAME").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.name),
                new Column().header("TYPE").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.type),
                new Column().header("STATE").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.state),
                new Column().header("MEMBERS").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.members)
        ));

        System.out.println(table);
    }

    private void printNames(List<GroupInfo> groups) {
        groups.forEach(group -> System.out.println(group.getGroupId()));
    }

    private void printYaml(List<GroupInfo> groups) {
        try {
            List<Map<String, Object>> groupList = new ArrayList<>();

            for (GroupInfo group : groups) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("groupId", group.getGroupId());
                data.put("type", group.getType());
                data.put("state", group.getState());
                data.put("members", group.getMemberCount());
                groupList.add(data);
            }

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            System.out.println(yamlMapper.writeValueAsString(groupList));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(List<GroupInfo> groups) {
        try {
            List<Map<String, Object>> groupList = new ArrayList<>();

            for (GroupInfo group : groups) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("groupId", group.getGroupId());
                data.put("type", group.getType());
                data.put("state", group.getState());
                data.put("members", group.getMemberCount());
                groupList.add(data);
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            System.out.println(jsonMapper.writeValueAsString(groupList));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private static class GroupRow {
        final String name;
        final String type;
        final String state;
        final String members;

        GroupRow(String name, String type, String state, String members) {
            this.name = name;
            this.type = type;
            this.state = state;
            this.members = members;
        }
    }
}
