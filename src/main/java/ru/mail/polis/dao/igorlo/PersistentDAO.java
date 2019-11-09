package ru.mail.polis.dao.igorlo;

import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PersistentDAO implements ExtendedDAO {
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
    private static final Logger log = LoggerFactory.getLogger(PersistentDAO.class);
    private final MemoryTablePool memoryTable;
    private final File rootDir;
    private final AtomicInteger fileIndex = new AtomicInteger(0);
    private final NavigableMap<Integer, Table> tables;
    private final Worker worker;

    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final int ALIVE = 1;
    static final int DEAD = 0;
    static final int LINK_SIZE = MODEL == 64 ? 8 : 4;
    static final int NUMBER_FIELDS_BYTEBUFFER = 7;
    static final ByteBuffer LEAST_KEY = ByteBuffer.allocate(0);
    static final String PREFIX = "FT";
    static final String SUFFIX = ".mydb";

    @Override
    public TableRow getRow(@NotNull final ByteBuffer key) throws IOException {
        final TableRow row = rowBy(key);
        if (row == null) {
            throw new LiteNoSuchElementException("Not found");
        }
        return row;
    }

    private TableRow rowBy(@NonNull final ByteBuffer key) throws IOException {
        final Iterator<TableRow> iter = rowIterator(key);
        TableRow row = null;
        if (iter.hasNext()) {
            row = iter.next();
            row = row.getKey().equals(key) ? row : null;
        }
        return row;
    }

    class Worker extends Thread {
        Worker() {
            super("worker");
        }

        @Override
        public void run() {
            boolean poisoned = false;
            boolean compacting = false;
            while (!poisoned && !isInterrupted()) {
                try {
                    final FlushTable table = memoryTable.takeToFlush();
                    dump(table.getTable(), table.getFileIndex());
                    compacting = table.isCompacting();
                    if (compacting) {
                        final Table compactingTable = Utilities.compactFiles(rootDir, tables);
                        tables.clear();
                        fileIndex.set(Utilities.START_FILE_INDEX + 1);
                        tables.put(fileIndex.get(), compactingTable);
                        memoryTable.compacted();
                    }
                    poisoned = table.isPoisonPill();
                    memoryTable.flushed(table.getFileIndex());
                } catch (InterruptedException e) {
                    log.error("InterruptedException during flushing file", e);
                    interrupt();
                } catch (IOException e) {
                    log.error("IOException during flushing file", e);
                }
            }
        }
    }

    /**
     * Creates LSM storage.
     *
     * @param maxHeap threshold of size of the memTable
     * @param rootDir the folder in which files will be written and read
     * @throws IOException if an I/O error is thrown by a File walker
     */
    public PersistentDAO(final long maxHeap, @NotNull final File rootDir) throws IOException {
        this.rootDir = rootDir;
        this.tables = new ConcurrentSkipListMap<>();
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(rootDir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith(PREFIX)
                        && file.getFileName().toString().endsWith(SUFFIX)) {
                    final Table fileTable = new SSTable(new File(rootDir, file.getFileName().toString()));
                    tables.put(fileIndex.getAndAdd(1), fileTable);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        this.memoryTable = new MemoryTablePool(maxHeap, fileIndex);
        this.worker = new Worker();
        this.worker.start();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(Utilities.aliveRowIterators(rowIterator(from)), TableRow::getRecord);
    }

    private Iterator<TableRow> rowIterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<TableRow>> iteratorList = Utilities.getListIterators(tables, memoryTable, from);
        return Utilities.getActualRowIterator(iteratorList);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memoryTable.upsert(key, value, fileIndex);
    }

    private void dump(@NotNull final Table table, final int fileIndex) throws IOException {
        final String fileTableName = PREFIX + fileIndex + SUFFIX;
        final File file = new File(rootDir, fileTableName);
        Utilities.write(file, table.iterator(LEAST_KEY));
        tables.put(fileIndex, new SSTable(file));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memoryTable.remove(key, fileIndex);
    }

    @Override
    public void close() throws IOException {
        memoryTable.close();
        try {
            worker.join();
        } catch (InterruptedException e) {
            log.error("InterruptedException during dao close", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Perform compaction.
     */
    @Override
    public void compact() throws IOException {
        memoryTable.compact();
    }
}
