package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public abstract class Mutation implements Comparable<Mutation> {
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
        if (result == 0) {
            result = getClass().getName().compareTo(o.getClass().getName());
            if (result == 0) {
                result = Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "timestamp=" + timestamp +
                ", visibility=" + visibility +
                '}';
    }
}
