package io.streamshub.clik.command.group;

import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "delete",
        description = "Delete one or more consumer groups"
)
public class DeleteGroupCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0..*",
            arity = "1..*",
            description = "Group ID(s)"
    )
    List<String> groupIds;

    @CommandLine.Option(
            names = {"-y", "--yes"},
            description = "Automatically confirm deletion without prompting"
    )
    boolean autoConfirm;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    GroupService groupService;

    @Override
    public Integer call() {
        // Prompt for confirmation unless --yes
        if (!autoConfirm) {
            String groupList = groupIds.size() == 1
                    ? "group \"" + groupIds.get(0) + "\""
                    : groupIds.size() + " groups";
            System.out.print("Delete " + groupList + "? This cannot be undone. [y/N]: ");
            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                System.out.println("Delete cancelled.");
                return 0;
            }
        }

        try (Admin admin = clientFactory.createAdminClient()) {
            groupService.deleteGroups(admin, groupIds);

            if (groupIds.size() == 1) {
                System.out.println("Group \"" + groupIds.get(0) + "\" deleted.");
            } else {
                System.out.println(groupIds.size() + " groups deleted.");
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to delete group(s): " + e.getMessage());
            return 1;
        }
    }
}
