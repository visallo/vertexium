package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

import java.io.Serializable;

public abstract class Mutation implements Comparable<Mutation>, Serializable {
    private final long objectCreationTimestamp = IncreasingTime.currentTimeMillis();
    private final long timestamp;
    private final Visibility visibility;

    protected Mutation(long timestamp, Visibility visibility) {
        this.timestamp = timestamp;
        this.visibility = visibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public int compareTo(Mutation o) {
        int result = Long.compare(getTimestamp(), o.getTimestamp());
        if (result != 0) {
            return result;
        }

        result = getClass().getName().compareTo(o.getClass().getName());
        if (result != 0) {
            return result;
        }

        // ensure mutation ordering when working with in memory graph
        result = Long.compare(objectCreationTimestamp, o.objectCreationTimestamp);
        if (result != 0) {
            return result;
        }

        return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
            "timestamp=" + timestamp +
            ", visibility=" + visibility +
            '}';
    }
}
