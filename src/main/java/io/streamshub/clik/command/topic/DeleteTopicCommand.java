package io.streamshub.clik.command.topic;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import picocli.CommandLine;

@CommandLine.Command(
        name = "delete",
        description = "Delete one or more topics"
)
public class DeleteTopicCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0..*",
            arity = "1..*",
            description = "Topic name(s)"
    )
    List<String> names;

    @CommandLine.Option(
            names = {"-y", "--yes"},
            description = "Automatically confirm deletion without prompting"
    )
    boolean autoConfirm;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        // Prompt for confirmation unless --yes
        if (!autoConfirm) {
            String topicList = names.size() == 1
                    ? "topic \"" + names.get(0) + "\""
                    : names.size() + " topics";
            out().print("Delete " + topicList + "? This cannot be undone. [y/N]: ");
            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                out().println("Delete cancelled.");
                return 0;
            }
        }

        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            topicService.deleteTopics(admin, names);

            if (names.size() == 1) {
                out().println("Topic \"" + names.get(0) + "\" deleted.");
            } else {
                out().println(names.size() + " topics deleted.");
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to delete topic(s): " + e.getMessage());
            return 1;
        }
    }
}
