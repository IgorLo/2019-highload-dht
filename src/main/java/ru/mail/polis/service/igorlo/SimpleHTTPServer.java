package ru.mail.polis.service.igorlo;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class SimpleHTTPServer extends HttpServer implements Service {

    private final DAO dao;

    public SimpleHTTPServer(@NotNull final DAO dao, final int port) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/entity")
    public Response entity(final Request request, @Param("id") final String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    final ByteBuffer value = dao.get(key);
                    final ByteBuffer buffer = value.duplicate();
                    final byte[] valueAsArray = new byte[buffer.remaining()];
                    buffer.get(valueAsArray);
                    return Response.ok(valueAsArray);

                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);

                case Request.METHOD_DELETE:
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);

            }
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response status(final Request request) {
        if (request.getMethod() == Request.METHOD_GET) {
            return Response.ok(Response.EMPTY);
        }
        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    private static HttpServerConfig getConfig(int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final var acceptor = new AcceptorConfig();
        acceptor.port = port;
        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.selectors = 4;
        return config;
    }


}