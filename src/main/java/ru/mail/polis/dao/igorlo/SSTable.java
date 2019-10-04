package ru.mail.polis.dao.igorlo;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * Allows convenient and easy access to previously stored data.
 */
class SSTable implements Closeable {
    private final int count;
    private final int fileIndex;
    private final FileChannel fc;
    private final File file;

    /**
     * Loads meta-data from saved file and provides access to rows stored in file.
     *
     * @param file previously saved file, for which table will be created.
     */
    SSTable(@NotNull final File file) throws IOException {
        this.file = file;
        final String indexString = file.getName().substring(
                PersistentDAO.PREFIX.length(),
                file.getName().length() - PersistentDAO.SUFFIX.length()
        );
        this.fileIndex = Integer.parseInt(indexString);
        this.fc = openRead(file);
        final ByteBuffer countBB = ByteBuffer.allocate(Integer.BYTES);
        fc.read(countBB, fc.size() - Integer.BYTES);
        countBB.rewind();
        this.count = countBB.getInt();
    }

    private static FileChannel openRead(@NotNull final File file) throws IOException {
        return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    @NotNull
    Iterator<TableRow> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            int index = getOffsetsIndex(from);

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public TableRow next() {
                assert hasNext();
                TableRow row = null;
                try {
                    row = getRowAt(index++);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return row;
            }
        };
    }

    private int getOffsetsIndex(@NotNull final ByteBuffer from) throws IOException {
        int left = 0;
        int right = count - 1;
        while (left <= right) {
            final int middle = left + (right - left) / 2;
            final int resCmp = from.compareTo(getKeyAt(middle));
            if (resCmp < 0) {
                right = middle - 1;
            } else if (resCmp > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private int getOffset(@NotNull final int i) throws IOException {
        final ByteBuffer offsetBB = ByteBuffer.allocate(Integer.BYTES);
        fc.read(offsetBB, fc.size() - Integer.BYTES - (long) Integer.BYTES * count + (long) Integer.BYTES * i);
        offsetBB.rewind();
        return offsetBB.getInt();
    }

    private ByteBuffer getKeyAt(@NotNull final int i) throws IOException {
        assert 0 <= i && i < count;
        final int offset = getOffset(i);
        return readByteBuffer(offset);
    }

    private ByteBuffer readByteBuffer(@NotNull final int offset) throws IOException {
        final ByteBuffer bufferSize = ByteBuffer.allocate(Integer.BYTES);
        fc.read(bufferSize, offset);
        bufferSize.rewind();
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize.getInt());
        fc.read(buffer, offset + Integer.BYTES);
        buffer.rewind();
        return buffer.slice();
    }

    private TableRow getRowAt(@NotNull final int i) throws IOException {
        assert 0 <= i && i < count;
        int offset = getOffset(i);

        //Key
        final ByteBuffer keyBB = readByteBuffer(offset);
        offset += Integer.BYTES + keyBB.remaining();

        //Status
        final ByteBuffer statusBB = ByteBuffer.allocate(Integer.BYTES);
        fc.read(statusBB, offset);
        statusBB.rewind();
        final int status = statusBB.getInt();
        offset += Integer.BYTES;

        if (status == PersistentDAO.DEAD) {
            return TableRow.of(fileIndex, keyBB.slice(), PersistentDAO.TOMBSTONE, status);
        } else {
            //Value
            final ByteBuffer valueBB = readByteBuffer(offset);
            return TableRow.of(fileIndex, keyBB.slice(), valueBB.slice(), status);
        }
    }

    static void writeToFile(@NotNull final File to,
            @NotNull final Iterator<TableRow> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(),
                                                        StandardOpenOption.CREATE_NEW,
                                                        StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (rows.hasNext()) {
                offsets.add(offset);
                final TableRow row = rows.next();

                //Key
                offset += writeByteBuffer(fileChannel, row.getKey());

                //Value
                if (row.isDead()) {
                    offset += fileChannel.write(fromInt(PersistentDAO.DEAD));
                } else {
                    offset += fileChannel.write(fromInt(PersistentDAO.ALIVE));
                    offset += writeByteBuffer(fileChannel, row.getValue());
                }
            }
            for (final Integer elemOffSets : offsets) {
                fileChannel.write(fromInt(elemOffSets));
            }
            fileChannel.write(fromInt(offsets.size()));
        }
    }

    private static ByteBuffer fromInt(@NotNull final int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).rewind();
    }

    private static int writeByteBuffer(@NotNull final FileChannel fileChannel, @NotNull final ByteBuffer buffer)
            throws IOException {
        int offset = 0;
        offset += fileChannel.write(fromInt(buffer.remaining()));
        offset += fileChannel.write(buffer);
        return offset;
    }

    @Override
    public void close() throws IOException {
        fc.close();
    }

    public void deleteFile() throws IOException {
        Files.delete(file.toPath());
    }
}
