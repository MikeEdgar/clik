package io.streamshub.clik.command.context;

import io.streamshub.clik.config.ContextService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.Scanner;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "delete",
        description = "Delete a context"
)
public class DeleteContextCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name"
    )
    String name;

    @CommandLine.Option(
            names = {"-f", "--force"},
            description = "Skip confirmation prompt"
    )
    boolean force;

    @Inject
    ContextService contextService;

    @Override
    public Integer call() {
        if (!contextService.contextExists(name)) {
            System.err.println("Error: Context \"" + name + "\" does not exist.");
            System.err.println();
            System.err.println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        // Prompt for confirmation unless --force
        if (!force) {
            System.out.print("Delete context \"" + name + "\"? This cannot be undone. [y/N]: ");
            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                System.out.println("Delete cancelled.");
                return 0;
            }
        }

        try {
            contextService.deleteContext(name);
            System.out.println("Context \"" + name + "\" deleted.");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: Failed to delete context: " + e.getMessage());
            return 1;
        }
    }
}
