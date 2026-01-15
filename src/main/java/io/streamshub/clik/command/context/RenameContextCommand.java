package io.streamshub.clik.command.context;

import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.config.ContextValidator;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

@CommandLine.Command(
        name = "rename",
        description = "Rename a context"
)
public class RenameContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Current context name",
            completionCandidates = NameCandidate.Context.class
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
            err().println("Error: Context \"" + oldName + "\" does not exist.");
            err().println();
            err().println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        // Validate new context name format
        if (!validator.isValidContextName(newName)) {
            err().println("Error: Invalid context name \"" + newName + "\" (contains invalid characters).");
            err().println();
            err().println("Context names must contain only letters, numbers, hyphens, and underscores.");
            return 1;
        }

        // Check new context doesn't already exist
        if (contextService.contextExists(newName)) {
            err().println("Error: Context \"" + newName + "\" already exists.");
            err().println();
            err().println("Choose a different name or delete the existing context first.");
            return 1;
        }

        try {
            contextService.renameContext(oldName, newName);
            out().println("Context \"" + oldName + "\" renamed to \"" + newName + "\".");
            return 0;
        } catch (Exception e) {
            err().println("Error: Failed to rename context: " + e.getMessage());
            return 1;
        }
    }
}
