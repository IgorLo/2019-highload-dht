package ru.mail.polis.service.igorlo;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.igorlo.TableRow;

final class ServiceUtilities {
    static final String PROXY_HEADER = "Is-Proxy: True";
    static final String TIME_HEADER = "Timestamp: ";

    private ServiceUtilities() {
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    static long getTimeStamp(@NotNull final Response response) {
        final String timeHeader = response.getHeader(TIME_HEADER);
        return timeHeader == null ? -1 : Long.parseLong(timeHeader);
    }

    static boolean validResponse(@NotNull final Response response) {
        return response.getHeaders()[0].equals(Response.NOT_FOUND) || response.getHeaders()[0].equals(Response.OK);
    }

    static Response responseFromRow(@NotNull final TableRow row){
        if (row.isDead()) {
            final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(ServiceUtilities.TIME_HEADER + row.getTime());
            return response;
        }
        final byte[] body = new byte[row.getValue().remaining()];
        row.getValue().get(body);
        final Response response = new Response(Response.OK, body);
        response.addHeader(ServiceUtilities.TIME_HEADER + row.getTime());
        return response;
    }

}
