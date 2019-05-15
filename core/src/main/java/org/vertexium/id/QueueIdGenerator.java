package org.vertexium.id;

import org.vertexium.VertexiumException;

import java.util.LinkedList;
import java.util.Queue;

public class QueueIdGenerator implements IdGenerator {
    private final Queue<String> ids = new LinkedList<>();

    @Override
    public String nextId() {
        synchronized (ids) {
            if (ids.size() == 0) {
                throw new VertexiumException("No ids in the queue to give out");
            }
            return ids.remove();
        }
    }

    public void push(String id) {
        ids.add(id);
    }
}
