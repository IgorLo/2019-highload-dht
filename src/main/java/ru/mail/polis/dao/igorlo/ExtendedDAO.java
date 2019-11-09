package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ExtendedDAO extends DAO {
    TableRow getRow(@NotNull final ByteBuffer key) throws IOException;
}
