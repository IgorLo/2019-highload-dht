package ru.mail.polis.dao.igorlo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Persistent and compactable DAO.
 */
public class PersistentDAO implements DAO {
    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final int ALIVE = 1;
    static final int DEAD = 0;
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
    private static final int LINK_SIZE = MODEL == 64 ? 8 : 4;
    private static final int NUMBER_FIELDS_BYTEBUFFER = 7;
    static final String PREFIX = "SSTABLE";
    static final String SUFFIX = ".db";
    private final SortedMap<ByteBuffer, TableRow> memTable = new TreeMap<>();
    private final long maxHeap;
    private final File rootFolder;
    private int currentFileIndex;
    private long currentHeap;
    private final List<SSTable> tables;

    /**
     * Creates persistent and compactable DAO in given folder and with given maxHeap.
     *
     * @param maxHeap max size of in-memory stored rows in bytes.
     * @param rootDir folder which will contain all flushed and compacted data.
     */
    public PersistentDAO(@NotNull final long maxHeap, @NotNull final File rootDir) throws IOException {
        this.maxHeap = maxHeap;
        this.rootFolder = rootDir;
        this.currentHeap = 0;
        this.tables = new ArrayList<>();
        this.currentFileIndex = 0;
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(rootDir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith(PREFIX)
                        && file.getFileName().toString().endsWith(SUFFIX)) {
                    final SSTable fileTable = new SSTable(new File(rootDir, file.getFileName().toString()));
                    tables.add(fileTable);
                    currentFileIndex++;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<TableRow>> tableIterators = new LinkedList<>();
        for (final SSTable fileT : tables) {
            tableIterators.add(fileT.iterator(from));
        }

        final Iterator<TableRow> memTableIterator = memTable.tailMap(from).values().iterator();
        tableIterators.add(memTableIterator);

        final Iterator<TableRow> result = getActualRowIterator(tableIterators);
        return Iterators.transform(result, TableRow::getRecord);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.put(key, TableRow.of(currentFileIndex, key, value, ALIVE));
        currentHeap += Integer.BYTES
                + (long) (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + (long) (value.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + Integer.BYTES;
        checkHeap();
    }

    private void dump() throws IOException {
        final String fileTableName = PREFIX + currentFileIndex + SUFFIX;
        currentFileIndex++;
        final File table = new File(rootFolder, fileTableName);
        SSTable.writeToFile(table, memTable.values().iterator());
        tables.add(new SSTable(table));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final TableRow removedRow = memTable.put(key, TableRow.of(currentFileIndex, key, TOMBSTONE, DEAD));
        if (removedRow == null) {
            currentHeap += Integer.BYTES
                    + (long) (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES;
        } else if (!removedRow.isDead()) {
            currentHeap -= removedRow.getValue().remaining();
        }
        checkHeap();
    }

    private void checkHeap() throws IOException {
        if (currentHeap >= maxHeap) {
            dump();
            currentHeap = 0;
            memTable.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if (currentHeap != 0) {
            dump();
        }
        for (final SSTable table : tables) {
            table.close();
        }
    }

    @Override
    public void compact() throws IOException {
        Compaction.compactFile(rootFolder, tables);
        currentFileIndex = tables.size();
    }

    /**
     * Returns all alive rows from tables in one iterator.
     *
     * @param tableIterators collection of iterators to merge.
     */
    public static Iterator<TableRow> getActualRowIterator(
            @NotNull final Collection<Iterator<TableRow>> tableIterators) {
        final Iterator<TableRow> mergingTableIterator = Iterators.mergeSorted(tableIterators, TableRow::compareTo);
        final Iterator<TableRow> collapsedIterator = Iters.collapseEquals(mergingTableIterator, TableRow::getKey);
        return Iterators.filter(collapsedIterator, row -> !row.isDead());
    }
}
