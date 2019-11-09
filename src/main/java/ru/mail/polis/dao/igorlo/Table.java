package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public interface Table {
    @NotNull
    Iterator<TableRow> iterator(@NotNull ByteBuffer from) throws IOException;

    /**
     * Inserts or updates value by given key.
     */
    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value,
            @NotNull AtomicInteger fileIndex) throws IOException;

    /**
     * Removes value by given key.
     */
    void remove(@NotNull ByteBuffer key,
                @NotNull AtomicInteger fileIndex) throws IOException;

    void clear();

    long sizeInBytes();
}
