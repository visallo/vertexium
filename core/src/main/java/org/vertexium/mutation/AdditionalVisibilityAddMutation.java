package org.vertexium.mutation;

public class AdditionalVisibilityAddMutation {
    private final String additionalVisibility;
    private final Object eventData;

    public AdditionalVisibilityAddMutation(String additionalVisibility, Object eventData) {
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
