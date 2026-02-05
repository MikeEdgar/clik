package io.streamshub.clik.support;

/**
 * Utility to find the root cause of a throwable
 */
public final class RootCause {

    private RootCause() {
    }

    /**
     * Utility to find the root cause of a throwable.
     *
     * @param thrown the Throwable, possibly null
     * @return the root cause of {@code thrown}, or {@code null} when thrown is
     *         {@code null}..
     */
    public static Throwable of(Throwable thrown) {
        if (thrown == null) {
            return null;
        }

        Throwable rootCause = thrown;

        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }

        return rootCause;
    }
}
