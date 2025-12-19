package io.streamshub.clik.command.context;

import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.config.ContextValidator;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "rename",
        description = "Rename a context"
)
public class RenameContextCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Current context name"
    )
    String oldName;

    @CommandLine.Parameters(
            index = "1",
            description = "New context name"
    )
    String newName;

    @Inject
    ContextService contextService;

    @Inject
    ContextValidator validator;

    @Override
    public Integer call() {
        // Validate old context exists
        if (!contextService.contextExists(oldName)) {
            System.err.println("Error: Context \"" + oldName + "\" does not exist.");
            System.err.println();
            System.err.println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        // Validate new context name format
        if (!validator.isValidContextName(newName)) {
            System.err.println("Error: Invalid context name \"" + newName + "\" (contains invalid characters).");
            System.err.println();
            System.err.println("Context names must contain only letters, numbers, hyphens, and underscores.");
            return 1;
        }

        // Check new context doesn't already exist
        if (contextService.contextExists(newName)) {
            System.err.println("Error: Context \"" + newName + "\" already exists.");
            System.err.println();
            System.err.println("Choose a different name or delete the existing context first.");
            return 1;
        }

        try {
            contextService.renameContext(oldName, newName);
            System.out.println("Context \"" + oldName + "\" renamed to \"" + newName + "\".");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: Failed to rename context: " + e.getMessage());
            return 1;
        }
    }
}
