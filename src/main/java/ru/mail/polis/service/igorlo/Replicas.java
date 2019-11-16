package ru.mail.polis.service.igorlo;

import org.jetbrains.annotations.NotNull;

public final class Replicas {
    private final int ack;
    private final int from;

    private Replicas(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public static Replicas quorum(final int count) {
        return new Replicas(count / 2 + 1, count);
    }

    /**
     * Method for parse string of replicas.
     *
     * @param replicas string "ack/from".
     * @return Replicas.
     */
    public static Replicas parse(@NotNull final String replicas) {
        if (!isCorrect(replicas)) {
            throw new IllegalArgumentException("Invalid request for ack/from");
        }
        final int ackInt;
        final int fromInt;
        try {
            final int iSeparator = replicas.indexOf('/');
            ackInt = Integer.parseInt(replicas.substring(0, iSeparator));
            fromInt = Integer.parseInt(replicas.substring(iSeparator + 1));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new NumberFormatException("Wrong number format for ack/from");
        }
        return new Replicas(ackInt, fromInt);
    }

    private static boolean isCorrect(@NotNull final String replicas) {
        return !replicas.isEmpty() && replicas.contains("/") && replicas.split("/").length == 2;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @Override
    public String toString() {
        return ack + "/" + from;
    }

}
