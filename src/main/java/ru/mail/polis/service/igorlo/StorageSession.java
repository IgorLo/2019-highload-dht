package ru.mail.polis.service.igorlo;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StorageSession extends HttpSession {
    private Chunks chunks;

    private static final byte[] SEPARATOR = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] NEW_LINE = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] END = "0\r\n\r\n".getBytes(Charsets.UTF_8);

    StorageSession(final Socket socket, final HttpServer server) {
        super(socket, server);
    }

    private static byte[] bufferToArray(@NotNull final ByteBuffer byteBuffer) {
        final ByteBuffer copy = byteBuffer.duplicate();
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }

    void stream(@NotNull final Iterator<Record> iterator) throws IOException {
        chunks = new Chunks(iterator);

        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);

        next();
    }

    private void next() throws IOException {
        while (chunks.hasNext() && queueHead == null) {
            final byte[] data = chunks.next();
            write(data, 0, data.length);
        }

        if (!chunks.hasNext()) {
            final byte[] end = chunks.end();
            write(end, 0, end.length);

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    server.handleRequest(handling, this);
                }
            }
        }
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    @SuppressWarnings("ClassCanBeStatic")
    private class Chunks {
        private final Iterator<Record> iterator;

        Chunks(@NotNull final Iterator<Record> iterator) {
            this.iterator = iterator;
        }

        /**
         * Get next chunk.
         *
         * @return array of bytes
         */
        public byte[] next() {
            assert hasNext();
            final Record record = iterator.next();
            final byte[] key = bufferToArray(record.getKey());
            final byte[] value = bufferToArray(record.getValue());
            final String length = Integer.toHexString(key.length
                    + NEW_LINE.length
                    + value.length);
            final int chunkLength = length.length()
                    + SEPARATOR.length
                    + key.length
                    + NEW_LINE.length
                    + value.length
                    + SEPARATOR.length;
            final byte[] chunk = new byte[chunkLength];
            final ByteBuffer chunkBuff = ByteBuffer.wrap(chunk);
            chunkBuff.put(length.getBytes(Charsets.UTF_8));
            chunkBuff.put(SEPARATOR);
            chunkBuff.put(key);
            chunkBuff.put(NEW_LINE);
            chunkBuff.put(value);
            chunkBuff.put(SEPARATOR);
            return chunk;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        byte[] end() {
            return END.clone();
        }
    }
}
