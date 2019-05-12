package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class DeleteAdditionalVisibilityMutation extends Mutation {
    private final Visibility additionalVisibility;
    private final Object eventData;

    public DeleteAdditionalVisibilityMutation(long timestamp, Visibility additionalVisibility, Object eventData) {
        super(timestamp, new Visibility(""));
        this.additionalVisibility = additionalVisibility;
        this.eventData = eventData;
    }

    public Visibility getAdditionalVisibility() {
        return additionalVisibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
