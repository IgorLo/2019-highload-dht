package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTable implements Table {
    private final SortedMap<ByteBuffer, TableRow> memTable = new ConcurrentSkipListMap<>();
    private final AtomicLong currentHeap = new AtomicLong(0);

    @NotNull
    @Override
    public Iterator<TableRow> iterator(@NotNull final ByteBuffer from) throws IOException {
        return memTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value,
                       @NotNull final AtomicInteger fileIndex) throws IOException {
        final TableRow previousRow = memTable.put(key, TableRow.of(fileIndex.get(), key, value, PersistentDAO.ALIVE));
        if (previousRow == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining()
                    + PersistentDAO.LINK_SIZE
                    + Integer.BYTES * PersistentDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (value.remaining()
                    + PersistentDAO.LINK_SIZE
                    + Integer.BYTES * PersistentDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES);
        } else {
            currentHeap.addAndGet(value.remaining() - previousRow.getValue().remaining());
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key,
                       @NotNull final AtomicInteger fileIndex) throws IOException {
        final TableRow removedRow = memTable.put(key, TableRow.of(fileIndex.get(), key, PersistentDAO.TOMBSTONE, PersistentDAO.DEAD));
        if (removedRow == null) {
            currentHeap.addAndGet(Integer.BYTES
                    + (long) (key.remaining()
                    + PersistentDAO.LINK_SIZE
                    + Integer.BYTES * PersistentDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (PersistentDAO.LINK_SIZE
                    + Integer.BYTES * PersistentDAO.NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES);
        } else if (!removedRow.isDead()) {
            currentHeap.addAndGet(-removedRow.getValue().remaining());
        }
    }

    @Override
    public void clear() {
        memTable.clear();
        currentHeap.set(0);
    }

    @Override
    public long sizeInBytes() {
        return currentHeap.get();
    }
}
