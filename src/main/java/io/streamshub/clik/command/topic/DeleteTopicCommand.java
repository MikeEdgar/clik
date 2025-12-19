package io.streamshub.clik.command.topic;

import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "delete",
        description = "Delete one or more topics"
)
public class DeleteTopicCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0..*",
            arity = "1..*",
            description = "Topic name(s)"
    )
    List<String> names;

    @CommandLine.Option(
            names = {"-f", "--force"},
            description = "Skip confirmation prompt"
    )
    boolean force;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        // Prompt for confirmation unless --force
        if (!force) {
            String topicList = names.size() == 1
                    ? "topic \"" + names.get(0) + "\""
                    : names.size() + " topics";
            System.out.print("Delete " + topicList + "? This cannot be undone. [y/N]: ");
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
            topicService.deleteTopics(admin, names);

            if (names.size() == 1) {
                System.out.println("Topic \"" + names.get(0) + "\" deleted.");
            } else {
                System.out.println(names.size() + " topics deleted.");
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to delete topic(s): " + e.getMessage());
            return 1;
        }
    }
}
