package io.streamshub.clik.command.completion;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import picocli.CommandLine;

@CommandLine.Command(name = "candidates", hidden = true)
public class CandidatesCompletionCommand implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Parameters(index = "0")
    String collectionName;

    @CommandLine.Unmatched
    List<String> unmmatched;

    @Inject
    ContextService contextService;

    @Inject
    KafkaClientFactory kafkaClientFactory;

    @Inject
    TopicService topicService;

    @Inject
    GroupService groupService;

    @Override
    public void run() {
        if ("context".equals(collectionName)) {
            printResults(contextService.listContexts());
        } else if ("topic".equals(collectionName)) {
            printResults(admin -> topicService.listTopics(admin, true));
        } else if ("group".equals(collectionName)) {
            printResults(admin -> groupService.listGroupIds(admin));
        }
    }

    private void printResults(Collection<String> results) {
        spec.commandLine().getOut().print(String.join(" ", results));
        spec.commandLine().getOut().print('\n');
        spec.commandLine().getOut().flush();
    }

    private void printResults(NameSupplier task) {
        try (Admin admin = kafkaClientFactory.createAdminClient(Optional.empty())) {
            printResults(task.apply(admin));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (Exception _) {
            // Ignore errors
        }
    }

    interface NameSupplier {
        Collection<String> apply(Admin admin) throws ExecutionException, InterruptedException;
    }
}
