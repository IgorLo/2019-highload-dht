package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTablePool implements Table, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MemoryTablePool.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final long maxHeap;
    private final NavigableMap<Integer, Table> tableForFlush;
    private volatile MemoryTable current;
    private final BlockingQueue<FlushTable> flushQueue;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final AtomicInteger fileIndex;

    MemoryTablePool(final long maxHeap, @NotNull final AtomicInteger fileIndex) {
        this.maxHeap = maxHeap;
        this.current = new MemoryTable();
        this.tableForFlush = new ConcurrentSkipListMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
        this.fileIndex = fileIndex;
    }

    @NotNull
    @Override
    public Iterator<TableRow> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<TableRow>> iteratorList;
        lock.readLock().lock();
        try {
            iteratorList = Utilities.getListIterators(tableForFlush, current, from);
        } finally {
            lock.readLock().unlock();
        }
        return Utilities.getActualRowIterator(iteratorList);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value,
                       @NotNull final AtomicInteger fileIndex) throws IOException {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.upsert(key, value, fileIndex);
        enqueueFlush(fileIndex);

    }

    @Override
    public void remove(@NotNull final ByteBuffer key,
                       @NotNull final AtomicInteger fileIndex) throws IOException {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.remove(key, fileIndex);
        enqueueFlush(fileIndex);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long sizeInBytes = 0;
            sizeInBytes += current.sizeInBytes();
            for (final Map.Entry<Integer, Table> entry : tableForFlush.entrySet()) {
                sizeInBytes += entry.getValue().sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        FlushTable table;
        lock.writeLock().lock();
        try {
            table = new FlushTable(current, fileIndex.get(), true);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(table);
        } catch (InterruptedException e) {
            log.error("InterruptedException during dao close", e);
            Thread.currentThread().interrupt();
        }
    }

    void compact() {
        if (!compacting.compareAndSet(false, true)) {
            return;
        }
        FlushTable table;
        lock.writeLock().lock();
        try {
            table = new FlushTable(current, fileIndex.getAndAdd(1), false, true);
            tableForFlush.put(table.getFileIndex(), table.getTable());
            current = new MemoryTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(table);
        } catch (InterruptedException e) {
            log.error("InterruptedException during dao compact", e);
            Thread.currentThread().interrupt();
        }
    }

    void compacted() {
        compacting.set(false);
    }

    private void enqueueFlush(@NotNull final AtomicInteger fileIndex) {
        if (current.sizeInBytes() >= maxHeap) {
            FlushTable table = null;
            int index = 0;
            lock.writeLock().lock();
            try {
                if (current.sizeInBytes() >= maxHeap) {
                    index = fileIndex.getAndAdd(1);
                    table = new FlushTable(current, index);
                    tableForFlush.put(index, current);
                    current = new MemoryTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (table != null) {
                try {
                    flushQueue.put(table);
                } catch (InterruptedException e) {
                    log.error("InterruptedException during enqueueFlush", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    FlushTable takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            tableForFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
