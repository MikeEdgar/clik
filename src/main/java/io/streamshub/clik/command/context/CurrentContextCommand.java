package io.streamshub.clik.command.context;

import java.util.Optional;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import picocli.CommandLine;

@CommandLine.Command(
        name = "current",
        description = "Display the current active context"
)
public class CurrentContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"--describe"},
            description = "Display full configuration of current context"
    )
    boolean describe;

    @Inject
    ContextService contextService;

    @Override
    public Integer call() {
        Optional<String> currentContext = contextService.getCurrentContext();

        if (currentContext.isEmpty()) {
            err().println("Error: No current context set.");
            err().println();
            err().println("Run 'clik context use <name>' to set a context.");
            return 1;
        }

        String contextName = currentContext.get();

        // Check if context still exists
        if (!contextService.contextExists(contextName)) {
            err().println("Error: Current context \"" + contextName + "\" no longer exists.");
            err().println();
            err().println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        if (describe) {
            out().println("Current context: " + contextName);
            out().println();
            out().println("Configuration:");

            try {
                ContextConfig config = contextService.loadContext(contextName);
                YAMLFactory yamlFactory = YAMLFactory.builder()
                        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                        .build();
                ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);

                String yamlOutput = yamlMapper.writeValueAsString(config);
                // Indent the YAML output
                String[] lines = yamlOutput.split("\n");
                for (String line : lines) {
                    out().println("  " + line);
                }
            } catch (Exception e) {
                err().println("Error: Failed to load context configuration: " + e.getMessage());
                return 1;
            }
        } else {
            out().println(contextName);
        }

        return 0;
    }
}
