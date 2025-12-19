package io.streamshub.clik.command.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "list",
        description = "List all available contexts"
)
public class ListContextsCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json, name (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @CommandLine.Option(
            names = {"--show-current"},
            description = "Highlight current active context (default: true)",
            defaultValue = "true"
    )
    boolean showCurrent;

    @Inject
    ContextService contextService;

    @Override
    public Integer call() {
        List<String> contexts = contextService.listContexts();

        if (contexts.isEmpty()) {
            System.out.println("No contexts found.");
            return 0;
        }

        // Sort contexts by name for consistent output order
        contexts.sort(String::compareTo);

        Optional<String> currentContext = showCurrent ? contextService.getCurrentContext() : Optional.empty();

        switch (outputFormat.toLowerCase()) {
            case "table":
                printTable(contexts, currentContext);
                break;
            case "name":
                printNames(contexts);
                break;
            case "yaml":
                printYaml(contexts, currentContext);
                break;
            case "json":
                printJson(contexts, currentContext);
                break;
            default:
                System.err.println("Error: Unknown output format: " + outputFormat);
                System.err.println("Valid formats: table, yaml, json, name");
                return 1;
        }

        return 0;
    }

    private void printTable(List<String> contexts, Optional<String> currentContext) {
        List<ContextRow> rows = new ArrayList<>();

        for (String context : contexts) {
            boolean isCurrent = currentContext.isPresent() && currentContext.get().equals(context);
            Optional<ContextConfig> config = contextService.getContext(context);

            String bootstrapServers = "";
            String security = "";

            if (config.isPresent()) {
                ContextConfig cfg = config.get();
                bootstrapServers = cfg.getCommon().getOrDefault("bootstrap.servers", "");
                security = cfg.getCommon().getOrDefault("security.protocol", "");

                // Truncate long bootstrap servers
                if (bootstrapServers.length() > 30) {
                    bootstrapServers = bootstrapServers.substring(0, 27) + "...";
                }
            }

            rows.add(new ContextRow(
                    isCurrent ? "*" : "",
                    context,
                    bootstrapServers,
                    security
            ));
        }

        String table = AsciiTable.getTable(AsciiTable.FANCY_ASCII, rows, List.of(
                new Column().header("CURRENT").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.CENTER).with(r -> r.current),
                new Column().header("NAME").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.name),
                new Column().header("BOOTSTRAP SERVERS").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.bootstrapServers),
                new Column().header("SECURITY").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.security)
        ));

        System.out.println(table);
    }

    private void printNames(List<String> contexts) {
        contexts.forEach(System.out::println);
    }

    private void printYaml(List<String> contexts, Optional<String> currentContext) {
        List<Map<String, Object>> contextList = new ArrayList<>();

        for (String context : contexts) {
            Optional<ContextConfig> config = contextService.getContext(context);
            if (config.isPresent()) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("name", context);
                data.put("current", currentContext.isPresent() && currentContext.get().equals(context));
                data.put("config", config.get());
                contextList.add(data);
            }
        }

        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            System.out.println(yamlMapper.writeValueAsString(contextList));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(List<String> contexts, Optional<String> currentContext) {
        List<Map<String, Object>> contextList = new ArrayList<>();

        for (String context : contexts) {
            Optional<ContextConfig> config = contextService.getContext(context);
            if (config.isPresent()) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("name", context);
                data.put("current", currentContext.isPresent() && currentContext.get().equals(context));
                data.put("config", config.get());
                contextList.add(data);
            }
        }

        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            System.out.println(jsonMapper.writeValueAsString(contextList));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private static class ContextRow {
        final String current;
        final String name;
        final String bootstrapServers;
        final String security;

        ContextRow(String current, String name, String bootstrapServers, String security) {
            this.current = current;
            this.name = name;
            this.bootstrapServers = bootstrapServers;
            this.security = security;
        }
    }
}
