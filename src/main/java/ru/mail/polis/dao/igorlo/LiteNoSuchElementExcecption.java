package ru.mail.polis.dao.igorlo;

import java.util.NoSuchElementException;

public class LiteNoSuchElementExcecption extends NoSuchElementException {

    private static final long serialVersionUID = 13L;

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
