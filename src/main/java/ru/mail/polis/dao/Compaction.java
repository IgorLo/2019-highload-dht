package ru.mail.polis.dao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

public final class Compaction {
    private static final String TMP = ".tmp";
    private static final int START_FILE_INDEX = 0;
    private static final ByteBuffer LEAST_KEY = ByteBuffer.allocate(0);

    /**
     * Private constructor to prevent users from creating an instance.
     */
    private Compaction() {
    }

    /**
     * Compacts multiple SStables to one and adds it to global list.
     *
     * @param rootDir tells where to save compacted file.
     * @param fileTables collection of tables to compact.
     */
    public static void compactFile(@NotNull final File rootDir,
            @NotNull final Collection<SSTable> fileTables) throws IOException {
        final List<Iterator<TableRow>> tableIterators = new LinkedList<>();
        for (final SSTable fileT : fileTables) {
            tableIterators.add(fileT.iterator(LEAST_KEY));
        }
        final Iterator<TableRow> filteredRow = PersistentDAO.getActualRowIterator(tableIterators);
        final File compactFileTmp = compact(rootDir, filteredRow);
        for (final SSTable fileTable : fileTables) {
            fileTable.close();
            fileTable.deleteFile();
        }
        fileTables.clear();
        final String fileDbName = PersistentDAO.PREFIX + START_FILE_INDEX + PersistentDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new SSTable(compactFileDb));
    }

    private static File compact(@NotNull final File rootDir,
            @NotNull final Iterator<TableRow> rows) throws IOException {
        final String fileTableName = PersistentDAO.PREFIX + START_FILE_INDEX + TMP;
        final File table = new File(rootDir, fileTableName);
        SSTable.writeToFile(table, rows);
        return table;
    }
}
