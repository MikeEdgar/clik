package io.streamshub.clik.command;

import java.io.PrintWriter;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

public class BaseCommand {

    @CommandLine.Spec
    CommandSpec commandSpec;

    protected PrintWriter out() {
        return commandSpec.commandLine().getOut();
    }

    protected PrintWriter err() {
        return commandSpec.commandLine().getErr();
    }
}
