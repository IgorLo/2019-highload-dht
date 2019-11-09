package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;

public final class TableRow implements Comparable<TableRow> {
    private final int index;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long time;
    private final int status;

    public long getTime() {
        return time;
    }

    private TableRow(final int index,
                     @NotNull final ByteBuffer key,
                     @NotNull final ByteBuffer value,
                     final int status,
                     final long time) {
        this.index = index;
        this.key = key;
        this.value = value;
        this.status = status;
        this.time = time;
    }

    public static TableRow of(final int index,
                              @NotNull final ByteBuffer key,
                              @NotNull final ByteBuffer value,
                              final int status,
                              final long time) {
        return new TableRow(index, key, value, status, time);
    }

    public static TableRow of(final int index,
                              @NotNull final ByteBuffer key,
                              @NotNull final ByteBuffer value,
                              final int status) {
        return new TableRow(index, key, value, status, Utilities.currentTimeNanos());
    }

    /**
     * Method for copy row.
     *
     * @return copy of this row.
     */
    public TableRow copy() {
        return new TableRow(index,
                key.duplicate().asReadOnlyBuffer(),
                value.duplicate().asReadOnlyBuffer(),
                status,
                time);
    }

    /**
     * Creates an object of class Record.
     *
     * @return Record
     */
    public Record getRecord() {
        if (isDead()) {
            return Record.of(key, PersistentDAO.TOMBSTONE);
        } else {
            return Record.of(key, value);
        }
    }

    public boolean isDead() {
        return status == PersistentDAO.DEAD;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int compareTo(@NotNull final TableRow o) {
        if (key.compareTo(o.getKey()) == 0) {
            return -Integer.compare(index, o.getIndex());
        }
        return key.compareTo(o.getKey());
    }
}
