package org.vertexium.mutation;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class AlterPropertyVisibility {
    private final String key;
    private final String name;
    private final long timestamp;
    private Visibility existingVisibility;
    private final Visibility visibility;

    public AlterPropertyVisibility(String key, String name, Visibility existingVisibility, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.existingVisibility = existingVisibility;
        this.visibility = visibility;
        this.timestamp = IncreasingTime.currentTimeMillis();

        // org.vertexium.inmemory.InMemoryGraph.alterElementPropertyVisibilities() requires an additional timestamp
        // to store the soft delete then the alter, this call will increment the counter
        IncreasingTime.advanceTime(1);
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getExistingVisibility() {
        return existingVisibility;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setExistingVisibility(Visibility existingVisibility) {
        this.existingVisibility = existingVisibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
