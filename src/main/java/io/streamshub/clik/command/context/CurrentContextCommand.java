package io.streamshub.clik.command.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.Optional;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "current",
        description = "Display the current active context"
)
public class CurrentContextCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"--show-config"},
            description = "Display full configuration of current context"
    )
    boolean showConfig;

    @Inject
    ContextService contextService;

    @Override
    public Integer call() {
        Optional<String> currentContext = contextService.getCurrentContext();

        if (currentContext.isEmpty()) {
            System.err.println("Error: No current context set.");
            System.err.println();
            System.err.println("Run 'clik context use <name>' to set a context.");
            return 1;
        }

        String contextName = currentContext.get();

        // Check if context still exists
        if (!contextService.contextExists(contextName)) {
            System.err.println("Error: Current context \"" + contextName + "\" no longer exists.");
            System.err.println();
            System.err.println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        if (showConfig) {
            System.out.println("Current context: " + contextName);
            System.out.println();
            System.out.println("Configuration:");

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
                    System.out.println("  " + line);
                }
            } catch (Exception e) {
                System.err.println("Error: Failed to load context configuration: " + e.getMessage());
                return 1;
            }
        } else {
            System.out.println(contextName);
        }

        return 0;
    }
}
