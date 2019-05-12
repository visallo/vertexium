package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.mutation.AdditionalVisibilityDeleteMutation;

public class DeleteAdditionalVisibilityEvent extends GraphEvent {
    private final Element element;
    private final String visibility;
    private final Object eventData;

    public DeleteAdditionalVisibilityEvent(
        Graph graph,
        Element element,
        AdditionalVisibilityDeleteMutation additionalVisibilityDeleteMutation
    ) {
        super(graph);
        this.element = element;
        this.visibility = additionalVisibilityDeleteMutation.getAdditionalVisibility();
        this.eventData = additionalVisibilityDeleteMutation.getEventData();
    }

    public DeleteAdditionalVisibilityEvent(Graph graph, Element element, String visibility, Object eventData) {
        super(graph);
        this.element = element;
        this.visibility = visibility;
        this.eventData = eventData;
    }

    public Element getElement() {
        return element;
    }

    public String getVisibility() {
        return visibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
