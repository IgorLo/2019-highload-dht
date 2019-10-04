package ru.mail.polis.dao.igorlo;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

final class TableRow implements Comparable<TableRow> {
    private final int index;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final int status;

    private TableRow(@NotNull final int index,
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value,
            @NotNull final int status) {
        this.index = index;
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public static TableRow of(@NotNull final int index,
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value,
            @NotNull final int status) {
        return new TableRow(index, key, value, status);
    }

    Record getRecord() {
        if (isDead()) {
            return Record.of(key, PersistentDAO.TOMBSTONE);
        } else {
            return Record.of(key, value);
        }
    }

    boolean isDead() {
        return status == PersistentDAO.DEAD;
    }

    ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    int getIndex() {
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
