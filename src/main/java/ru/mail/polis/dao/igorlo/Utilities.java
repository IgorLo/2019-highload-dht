package ru.mail.polis.dao.igorlo;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map;

final class Utilities {
    private static final String TMP = ".tmp";
    private static final ByteBuffer LEAST_KEY = PersistentDAO.LEAST_KEY;
    static final int START_FILE_INDEX = 0;
    private static int timeCounter;
    private static long lastMillis;

    private Utilities() {
    }

    /**
     * Compact files. Since deletions and changes accumulate, we have to collapse
     * all these changes, on the one hand, reducing the search time, on the
     * other - reducing the required storage space. Single file will be created
     * in which the most relevant data will be stored and the rest will be deleted.
     *
     * @param rootDir    base directory
     * @param fileTables list file tables that will collapse
     * @throws IOException if an I/O error is thrown by FileTable.iterator
     */
    static Table compactFiles(@NotNull final File rootDir,
                              @NotNull final NavigableMap<Integer, Table> fileTables) throws IOException {
        final List<Iterator<TableRow>> tableIterators = new ArrayList<>();
        for (final Table fileT : fileTables.values()) {
            tableIterators.add(fileT.iterator(LEAST_KEY));
        }
        final Iterator<TableRow> filteredRow = getActualRowIterator(tableIterators);
        final File compactFileTmp = compact(rootDir, filteredRow);
        for (final Map.Entry<Integer, Table> entry :
                fileTables.entrySet()) {
            entry.getValue().clear();
        }
        final String fileDbName = PersistentDAO.PREFIX + START_FILE_INDEX + PersistentDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return new SSTable(compactFileDb);
    }

    private static File compact(@NotNull final File rootDir,
                                @NotNull final Iterator<TableRow> rows) throws IOException {
        final String fileTableName = PersistentDAO.PREFIX + START_FILE_INDEX + TMP;
        final File table = new File(rootDir, fileTableName);
        Utilities.write(table, rows);
        return table;
    }

    /**
     * Writes data to file. First writes all row: key length, key, status (DEAD, ALIVE),
     * value length, value. Then writes offsets array, and then writes amount of row.
     *
     * @param to   file being recorded
     * @param rows strings to be written to file
     * @throws IOException if an I/O error is thrown by a write method
     */
    static void write(@NotNull final File to,
                      @NotNull final Iterator<TableRow> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Integer> offsets = writeRows(fileChannel, rows);
            writeOffsets(fileChannel, offsets);
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }

    private static int writeByteBuffer(@NotNull final FileChannel fc,
                                       @NotNull final ByteBuffer buffer) throws IOException {
        int offset = 0;
        offset += fc.write(Bytes.fromInt(buffer.remaining()));
        offset += fc.write(buffer);
        return offset;
    }

    private static void writeOffsets(@NotNull final FileChannel fc,
                                     @NotNull final List<Integer> offsets) throws IOException {
        for (final Integer elemOffSets : offsets) {
            fc.write(Bytes.fromInt(elemOffSets));
        }
    }

    private static List<Integer> writeRows(@NotNull final FileChannel fc,
                                           @NotNull final Iterator<TableRow> rows) throws IOException {
        final List<Integer> offsets = new ArrayList<>();
        int offset = 0;
        while (rows.hasNext()) {
            offsets.add(offset);
            final TableRow row = rows.next();

            //Key
            offset += writeByteBuffer(fc, row.getKey());

            //Value
            if (row.isDead()) {
                offset += fc.write(Bytes.fromInt(PersistentDAO.DEAD));
            } else {
                offset += fc.write(Bytes.fromInt(PersistentDAO.ALIVE));
                offset += writeByteBuffer(fc, row.getValue()); // row.getValue().getData()
            }
            offset += fc.write(Bytes.fromLong(row.getTime()));
        }
        return offsets;
    }

    /**
     * Get merge sorted, collapse equals, without dead row iterator.
     *
     * @param tableIterators collection MyTableIterator
     * @return Row Iterator
     */
    static Iterator<TableRow> getActualRowIterator(@NotNull final Collection<Iterator<TableRow>> tableIterators) {
        final Iterator<TableRow> mergingTableIterator = Iterators.mergeSorted(tableIterators, TableRow::compareTo);
        return Iters.collapseEquals(mergingTableIterator, TableRow::getKey);
    }

    static Iterator<TableRow> aliveRowIterators(@NotNull final Iterator<TableRow> iterator) {
        return Iterators.filter(iterator, row -> !row.isDead());
    }

    static List<Iterator<TableRow>> getListIterators(@NotNull final NavigableMap<Integer, Table> tables,
                                                     @NotNull final Table memTable,
                                                     @NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<TableRow>> tableIterators = new ArrayList<>();
        for (final Table fileT : tables.descendingMap().values()) {
            tableIterators.add(fileT.iterator(from));
        }
        final Iterator<TableRow> memTableIterator = memTable.iterator(from);
        tableIterators.add(memTableIterator);
        return tableIterators;
    }

    static long currentTimeNanos() {
        synchronized (Utilities.class) {
            final var millis = System.currentTimeMillis();
            if (lastMillis != millis) {
                lastMillis = millis;
                timeCounter = 0;
            }
            return lastMillis * 1_000_000 + timeCounter++;
        }
    }

    private static final class Bytes {
        private Bytes() {
            // Not instantiatable
        }

        static ByteBuffer fromInt(final int i) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
        }

        static ByteBuffer fromLong(final long i) {
            return ByteBuffer.allocate(Long.BYTES).putLong(i).rewind();
        }
    }

}
