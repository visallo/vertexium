package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;
import org.vertexium.mutation.AdditionalVisibilityAddMutation;

public class AddAdditionalVisibilityEvent extends GraphEvent {
    private final Element element;
    private final String visibility;
    private final Object eventData;

    public AddAdditionalVisibilityEvent(
        Graph graph,
        Element element,
        AdditionalVisibilityAddMutation additionalVisibilityAddMutation
    ) {
        super(graph);
        this.element = element;
        this.visibility = additionalVisibilityAddMutation.getAdditionalVisibility();
        this.eventData = additionalVisibilityAddMutation.getEventData();
    }

    public AddAdditionalVisibilityEvent(Graph graph, Element element, String visibility, Object eventData) {
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
