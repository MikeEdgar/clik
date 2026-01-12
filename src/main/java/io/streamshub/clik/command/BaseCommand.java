package io.streamshub.clik.command;

import java.io.PrintWriter;
import java.util.function.Function;

import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;

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

    protected static <T> ColumnData<T> column(String name, HorizontalAlign dataAlign, Function<T, String> data) {
        return new Column().header(name).headerAlign(HorizontalAlign.LEFT).dataAlign(dataAlign).with(data);
    }
}
