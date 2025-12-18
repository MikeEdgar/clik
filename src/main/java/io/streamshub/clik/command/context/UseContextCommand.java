package io.streamshub.clik.command.context;

import io.streamshub.clik.config.ContextService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "use",
        description = "Switch to a different context (set as current)"
)
public class UseContextCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name"
    )
    String name;

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

        try {
            contextService.setCurrentContext(name);
            System.out.println("Switched to context \"" + name + "\".");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: Failed to switch context: " + e.getMessage());
            return 1;
        }
    }
}
