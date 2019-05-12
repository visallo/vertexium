package org.vertexium.mutation;

import org.vertexium.Visibility;

public class AdditionalVisibilityAddMutation {
    private final Visibility additionalVisibility;
    private final Object eventData;

    public AdditionalVisibilityAddMutation(Visibility additionalVisibility, Object eventData) {
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
