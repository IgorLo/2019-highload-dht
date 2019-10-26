package ru.mail.polis.service.igorlo;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Topology based on hashing for clustering.
 */
public class HashingTopology{
    private final int range;
    private final Map<Integer, String> mappedNodes;
    private final String me;

    /**
     * Hashing topology constructor, creating a topology for current set of nodes.
     *
     * @param nodes - nodes of our topology.
     * @param me - URL for node itself.
     * @param range - range that the node is responsible for.
     */
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

    String primaryFor(@NotNull final ByteBuffer key) {
        return mappedNodes.get(hashCode(key));
    }

    boolean isMe(@NotNull final String node) {
        return me.equals(node);
    }

    Set<String> all() {
        return new TreeSet<>(mappedNodes.values());
    }

    private int hashCode(final Object o) {
        return o.hashCode() % range;
    }
}
