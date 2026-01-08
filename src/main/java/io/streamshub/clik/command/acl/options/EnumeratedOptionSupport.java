package io.streamshub.clik.command.acl.options;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

import picocli.CommandLine;

abstract class EnumeratedOptionSupport implements CommandLine.ITypeConverter<String>, Iterable<String> {
    private final Collection<String> values;

    @SafeVarargs
    static <E extends Enum<E>> Collection<String> without(Enum<E> firstExcluded, Enum<E>... otherExcluded) {
        return Arrays.stream(firstExcluded.getClass().getEnumConstants())
                .filter(Predicate.not(firstExcluded::equals))
                .filter(Predicate.not(Arrays.asList(otherExcluded)::contains))
                .map(Enum::name)
                .toList();
    }

    protected EnumeratedOptionSupport(Collection<String> values) {
        this.values = values;
    }

    @Override
    public String convert(String value) throws Exception {
        return values.stream()
                .filter(v -> v.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new CommandLine.TypeConversionException("Must be one of " + values));
    }

    @Override
    public Iterator<String> iterator() {
        return values.iterator();
    }
}
