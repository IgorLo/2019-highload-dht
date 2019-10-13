package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.igorlo.PersistentDAO.TOMBSTONE;

public class MemoryTablePool implements Table, Closeable {
    static final ByteBuffer LOWEST_KEY = ByteBuffer.allocate(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemoryTable current;
    private final NavigableMap<Long, Table> pendingToFlush;
    private final NavigableMap<Long, Iterator<TableRow>> pendingToCompact;
    private final BlockingQueue<FlushTable> flushQueue;
    private long index;

    @NotNull private final ExecutorService flusher;
    @NotNull private final Runnable flushingTask;

    private final long flushThresholdInBytes;
    private final AtomicBoolean isClosed;

    /**
     * Pool of mem table to flush.
     *
     * @param flushThresholdInBytes - limit for flushing tables.
     * @param startIndex - first index to be used for tables.
     **/
    public MemoryTablePool(final long flushThresholdInBytes,
                        final long startIndex,
                        final int nThreadsToFlush,
                        @NotNull final Runnable flushingTask) {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.current = new MemoryTable();
        this.pendingToFlush = new TreeMap<>();
        this.index = startIndex;
        this.flushQueue = new ArrayBlockingQueue<>(nThreadsToFlush + 1);
        this.isClosed = new AtomicBoolean();
        this.pendingToCompact = new TreeMap<>();

        this.flusher = Executors.newFixedThreadPool(nThreadsToFlush);
        this.flushingTask = flushingTask;
    }

    @NotNull
    @Override
    public Iterator<TableRow> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<TableRow>> iterators;
        try {
            iterators = Table.combineTables(current, pendingToFlush, from);
        } finally {
            lock.readLock().unlock();
        }
        return Table.transformRows(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed!");
        }
        setToFlush(key);
        lock.readLock().lock();
        try {
            current.upsert(key, value);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed!");
        }
        setToFlush(key);
        lock.readLock().lock();
        try {
            current.remove(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void setToFlush(@NotNull final ByteBuffer key) throws IOException {
        if (current.sizeInBytes()
                + TableRow.getSizeOfFlushedRow(key, TOMBSTONE) >= flushThresholdInBytes) {
            lock.writeLock().lock();
            FlushTable tableToFlush = null;
            try {
                if (current.sizeInBytes()
                        + TableRow.getSizeOfFlushedRow(key, TOMBSTONE) >= flushThresholdInBytes) {
                    tableToFlush = FlushTable.of(current.iterator(LOWEST_KEY), index);
                    pendingToFlush.put(index, current);
                    index++;
                    current = new MemoryTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (tableToFlush != null) {
                try {
                    flushQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                flusher.execute(flushingTask);
            }
        }
    }

    private void setCompactTableToFlush(@NotNull final Iterator<TableRow> rows) throws IOException {
        lock.writeLock().lock();
        FlushTable tableToFlush;
        try {
            tableToFlush = new FlushTable
                    .Builder(rows, index)
                    .isCompactTable()
                    .build();
            index++;
            pendingToCompact.put(index, rows);
            current = new MemoryTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flusher.execute(flushingTask);
    }

    @NotNull
    public FlushTable takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    /**
     * Mark mem table as flushed and remove her from map storage of tables.
     * @param serialNumber - index of removable table.
     * */
    public void flushed(final long serialNumber) {
        lock.writeLock().lock();
        try {
            pendingToFlush.remove(serialNumber);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compact values from all tables with current table.
     * @param ssTables - all tables from root.
     * */
    public void compact(@NotNull final NavigableMap<Long, Table> ssTables) throws IOException {
        lock.readLock().lock();
        final List<Iterator<TableRow>> iterators;
        try {
            iterators = Table.combineTables(current, ssTables, LOWEST_KEY);
        } finally {
            lock.readLock().unlock();
        }
        setCompactTableToFlush(Table.transformRows(iterators));
    }

    /**
     * Compacted.
     * @param serialNumber - index of compacted table.
     * */
    public void compacted(final long serialNumber) {
        lock.writeLock().lock();
        try {
            pendingToCompact.remove(serialNumber);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long sizeInBytes = current.sizeInBytes();
            for (final var table : pendingToFlush.values()) {
                sizeInBytes += table.sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long serialNumber() {
        lock.readLock().lock();
        try {
            return index;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        FlushTable tableToFlush;
        try {
            tableToFlush = new FlushTable
                    .Builder(current.iterator(LOWEST_KEY), index)
                    .poisonPill()
                    .build();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flusher.execute(flushingTask);
        stopFlushing();
    }

    private void stopFlushing() {
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(1, TimeUnit.MINUTES)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
