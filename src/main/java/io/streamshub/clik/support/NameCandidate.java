package io.streamshub.clik.support;

import java.util.Iterator;
import java.util.List;

public abstract class NameCandidate implements Iterable<String> {

    private final String category;

    private NameCandidate() {
        this.category = getClass().getSimpleName().toLowerCase();
    }

    @Override
    public Iterator<String> iterator() {
        return List.of("${clik-completion:%s}".formatted(category)).iterator();
    }

    public static class Context extends NameCandidate {
    }

    public static class Group extends NameCandidate {
    }

    public static class Topic extends NameCandidate {
    }
}
