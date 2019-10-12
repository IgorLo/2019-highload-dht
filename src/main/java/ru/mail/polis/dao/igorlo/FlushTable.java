package ru.mail.polis.dao.igorlo;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class FlushTable {
    @NotNull private final Iterator<TableRow> rows;
    private final long index;
    private final boolean poisonPill;
    private final boolean isCompactTable;

    public static class Builder {
        @NotNull private final Iterator<TableRow> rows;
        private final long index;

        private boolean poisonPill;
        private boolean isCompactTable;

        public Builder(@NotNull final Iterator<TableRow> rows,
                       final long index) {
            this.rows = rows;
            this.index = index;
        }

        public Builder poisonPill() {
            poisonPill = true;
            return this;
        }

        public Builder isCompactTable() {
            isCompactTable = true;
            return this;
        }

        public FlushTable build() {
            return new FlushTable(this);
        }
    }

    private FlushTable(@NotNull final Builder builder) {
        this.index = builder.index;
        this.rows = builder.rows;
        this.poisonPill = builder.poisonPill;
        this.isCompactTable = builder.isCompactTable;
    }

    private FlushTable(@NotNull final Iterator<TableRow> rows,
                         final long index) {
        this.index = index;
        this.rows = rows;
        this.poisonPill = false;
        this.isCompactTable = false;
    }

    public static FlushTable of(@NotNull final Iterator<TableRow> rows,
                                  final long serialNumber) {
        return new FlushTable(rows, serialNumber);
    }

    public long getIndex() {
        return index;
    }

    @NotNull
    public Iterator<TableRow> getTable() {
        return rows;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }

    public boolean isCompactTable() {
        return isCompactTable;
    }
}
