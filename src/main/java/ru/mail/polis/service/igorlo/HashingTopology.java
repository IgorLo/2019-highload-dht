package ru.mail.polis.service.igorlo;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

public class HashingTopology{
    private final int range;
    private final HashMap<Integer, String> mappedNodes;
    private final String me;

    public HashingTopology(@NotNull final Set<String> nodes,
                           @NotNull final String me,
                           final int range) {
        this.range = range;
        this.mappedNodes = new HashMap<>(2 * range + 1);
        this.me = me;
        int offset = 0;
        for (final String node : nodes) {
            for (int i = -range + offset; i <= range; i += nodes.size()) {
                this.mappedNodes.put(i, node);
            }
            offset++;
        }
    }

    String primaryFor(@NotNull ByteBuffer key) {
        return mappedNodes.get(hashCode(key));
    }

    boolean isMe(@NotNull String node) {
        return me.equals(node);
    }

    Set<String> all() {
        return new TreeSet<>(mappedNodes.values());
    }

    private int hashCode(Object o) {
        return o.hashCode() % range;
    }
}
