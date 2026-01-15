package io.streamshub.clik.command.completion;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

@CommandLine.Command(name = "generate",
    mixinStandardHelpOptions = true,
    description = {
        "Generate bash/zsh completion script for ${ROOT-COMMAND-NAME:-the root command of this command}.",
        "Run the following command to give `${ROOT-COMMAND-NAME:-$PARENTCOMMAND}` TAB completion in the current shell:",
        "",
        "  source <(${PARENT-COMMAND-FULL-NAME:-$PARENTCOMMAND} ${COMMAND-NAME})",
        ""},
    optionListHeading = "Options:%n",
    helpCommand = true
)
public class GenerateCompletionCommand implements Runnable {

    @Option(names = {"-n", "--name"},
            paramLabel = "name",
            description = """
                    Optionally specify the name of the command to create a completion script for. \
                    When omitted, the root command (program) name is used."""
    )
    String commandName;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    public void run() {
        CommandLine rootCLI = spec.root().commandLine();

        if (commandName == null) {
            commandName = rootCLI.getCommandName();
        }

        String script = AutoComplete.bash(commandName, rootCLI);
        Pattern pattern = compile(quote("\"${clik-completion:") + "([^}]+)" + quote("}\""));
        Matcher matcher = pattern.matcher(script);

        script = matcher.replaceAll(result -> "\\$\\(%s completion candidates %s \"\\${COMP_WORDS[@]}\" 2>/dev/null || echo \"\"\\)"
                .formatted(commandName, result.group(1)));

        // not PrintWriter.println: scripts with Windows line separators fail in strange ways!
        spec.commandLine().getOut().print(script);
        spec.commandLine().getOut().print('\n');
        spec.commandLine().getOut().flush();
    }
}
