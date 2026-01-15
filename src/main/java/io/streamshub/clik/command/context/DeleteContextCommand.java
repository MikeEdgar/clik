package io.streamshub.clik.command.context;

import java.util.Scanner;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

@CommandLine.Command(
        name = "delete",
        description = "Delete a context"
)
public class DeleteContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name",
            completionCandidates = NameCandidate.Context.class
    )
    String name;

    @CommandLine.Option(
            names = {"-y", "--yes"},
            description = "Automatically confirm deletion without prompting"
    )
    boolean autoConfirm;

    @Inject
    ContextService contextService;

    @Override
    public Integer call() {
        if (!contextService.contextExists(name)) {
            err().println("Error: Context \"" + name + "\" does not exist.");
            err().println();
            err().println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        // Prompt for confirmation unless --yes
        if (!autoConfirm) {
            out().print("Delete context \"" + name + "\"? This cannot be undone. [y/N]: ");
            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                out().println("Delete cancelled.");
                return 0;
            }
        }

        try {
            contextService.deleteContext(name);
            out().println("Context \"" + name + "\" deleted.");
            return 0;
        } catch (Exception e) {
            err().println("Error: Failed to delete context: " + e.getMessage());
            return 1;
        }
    }
}
