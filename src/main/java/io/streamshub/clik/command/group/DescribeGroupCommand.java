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
import io.streamshub.clik.kafka.model.GroupMemberInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(
        name = "describe",
        description = "Display detailed information about a consumer group"
)
public class DescribeGroupCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Group ID"
    )
    String groupId;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    GroupService groupService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient()) {
            GroupInfo group = groupService.describeGroup(admin, groupId);

            if (group == null) {
                System.err.println("Error: Group \"" + groupId + "\" not found.");
                System.err.println();
                System.err.println("Run 'clik group list' to see available groups.");
                return 1;
            }

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(group);
                    break;
                case "yaml":
                    printYaml(group);
                    break;
                case "json":
                    printJson(group);
                    break;
                default:
                    System.err.println("Error: Unknown output format: " + outputFormat);
                    System.err.println("Valid formats: table, yaml, json");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to describe group: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(GroupInfo group) {
        System.out.println("Group: " + group.getGroupId());
        System.out.println("Type: " + group.getType());
        System.out.println("State: " + group.getState());
        if (group.getProtocol() != null) {
            System.out.println("Protocol: " + group.getProtocol());
        }
        System.out.println();

        if (group.getMembers() != null && !group.getMembers().isEmpty()) {
            System.out.println("Members:");
            List<MemberRow> rows = new ArrayList<>();
            for (GroupMemberInfo member : group.getMembers()) {
                String partitions = formatPartitions(member);
                rows.add(new MemberRow(
                        member.getMemberId(),
                        member.getHost(),
                        member.getClientId(),
                        partitions
                ));
            }

            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                    new Column().header("MEMBER ID").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.memberId),
                    new Column().header("HOST").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.host),
                    new Column().header("CLIENT ID").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.clientId),
                    new Column().header("PARTITIONS").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.partitions)
            ));

            System.out.println(table);
            System.out.println();
        }

        if (group.getOffsets() != null && !group.getOffsets().isEmpty()) {
            System.out.println("Topic Lag:");
            List<OffsetRow> rows = new ArrayList<>();
            for (OffsetLagInfo offset : group.getOffsets()) {
                rows.add(new OffsetRow(
                        offset.getTopic(),
                        String.valueOf(offset.getPartition()),
                        offset.getCurrentOffset() != null ? String.valueOf(offset.getCurrentOffset()) : "-",
                        offset.getLogEndOffset() != null ? String.valueOf(offset.getLogEndOffset()) : "-",
                        offset.getLag() != null ? String.valueOf(offset.getLag()) : "-"
                ));
            }

            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                    new Column().header("TOPIC").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.topic),
                    new Column().header("PARTITION").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.partition),
                    new Column().header("CURRENT OFFSET").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.currentOffset),
                    new Column().header("LOG END OFFSET").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.logEndOffset),
                    new Column().header("LAG").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.lag)
            ));

            System.out.println(table);
        }
    }

    private String formatPartitions(GroupMemberInfo member) {
        if (member.getAssignments() == null || member.getAssignments().isEmpty()) {
            return "-";
        }

        return member.getAssignments().stream()
                .map(assignment -> {
                    String partList = assignment.getPartitions().stream()
                            .map(String::valueOf)
                            .collect(Collectors.joining(","));
                    return assignment.getTopic() + "(" + partList + ")";
                })
                .collect(Collectors.joining(", "));
    }

    private void printYaml(GroupInfo group) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            System.out.print(yamlMapper.writeValueAsString(group));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(GroupInfo group) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            System.out.println(jsonMapper.writeValueAsString(group));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private static class MemberRow {
        final String memberId;
        final String host;
        final String clientId;
        final String partitions;

        MemberRow(String memberId, String host, String clientId, String partitions) {
            this.memberId = memberId;
            this.host = host;
            this.clientId = clientId;
            this.partitions = partitions;
        }
    }

    private static class OffsetRow {
        final String topic;
        final String partition;
        final String currentOffset;
        final String logEndOffset;
        final String lag;

        OffsetRow(String topic, String partition, String currentOffset, String logEndOffset, String lag) {
            this.topic = topic;
            this.partition = partition;
            this.currentOffset = currentOffset;
            this.logEndOffset = logEndOffset;
            this.lag = lag;
        }
    }
}
