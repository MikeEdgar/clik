package io.streamshub.clik.command.context;

import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.ScalarStyle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ConfigurationLoader;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import picocli.CommandLine;

@CommandLine.Command(
        name = "describe",
        description = "Display detailed configuration for a specific context"
)
public class DescribeContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name"
    )
    String name;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: yaml, json, properties (default: yaml)",
            defaultValue = "yaml"
    )
    String outputFormat;

    @Inject
    ContextService contextService;

    @Inject
    ConfigurationLoader configLoader;

    @Override
    public Integer call() {
        if (!contextService.contextExists(name)) {
            err().println("Error: Context \"" + name + "\" does not exist.");
            err().println();
            err().println("Run 'clik context list' to see available contexts.");
            return 1;
        }

        try {
            ContextConfig config = contextService.loadContext(name);

            switch (outputFormat.toLowerCase()) {
                case "yaml":
                    printYaml(config);
                    break;
                case "json":
                    printJson(config);
                    break;
                case "properties":
                    printProperties(config);
                    break;
                default:
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: yaml, json, properties");
                    return 1;
            }

            return 0;
        } catch (Exception e) {
            err().println("Error: Failed to load context: " + e.getMessage());
            return 1;
        }
    }

    private void printYaml(ContextConfig config) {
        try {
            DumperOptions options = new DumperOptions();
            options.setDefaultScalarStyle(ScalarStyle.LITERAL); // '|'

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .dumperOptions(options)
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().print(yamlMapper.writeValueAsString(config));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(ContextConfig config) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(config));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private void printProperties(ContextConfig config) {
        String propertiesOutput = configLoader.toPropertiesFormat(config);
        out().print(propertiesOutput);
    }
}
