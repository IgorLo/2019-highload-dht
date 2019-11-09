package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

public class LiteNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = 13L;

    public LiteNoSuchElementException(@NotNull final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}