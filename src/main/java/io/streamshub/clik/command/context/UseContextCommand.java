package io.streamshub.clik.command.context;

import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

@CommandLine.Command(
        name = "use",
        description = "Switch to a different context (set as current)"
)
public class UseContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name",
            completionCandidates = NameCandidate.Context.class
    )
    String name;

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

        try {
            contextService.setCurrentContext(name);
            out().println("Switched to context \"" + name + "\".");
            return 0;
        } catch (Exception e) {
            err().println("Error: Failed to switch context: " + e.getMessage());
            return 1;
        }
    }
}
