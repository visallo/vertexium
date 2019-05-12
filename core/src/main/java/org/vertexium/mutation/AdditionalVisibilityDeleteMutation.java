package org.vertexium.mutation;

public class AdditionalVisibilityDeleteMutation {
    private final String additionalVisibility;
    private final Object eventData;

    public AdditionalVisibilityDeleteMutation(String additionalVisibility, Object eventData) {
        this.additionalVisibility = additionalVisibility;
        this.eventData = eventData;
    }

    public String getAdditionalVisibility() {
        return additionalVisibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
