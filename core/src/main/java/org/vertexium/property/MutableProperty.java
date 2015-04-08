package org.vertexium.property;

import org.vertexium.Property;
import org.vertexium.Visibility;

public abstract class MutableProperty extends Property {
    public abstract void setValue(Object value);

    public abstract void setTimestamp(long timestamp);

    public abstract void setVisibility(Visibility visibility);

    public abstract void addHiddenVisibility(Visibility visibility);

    public abstract void removeHiddenVisibility(Visibility visibility);

    protected abstract void updateMetadata(Property property);

    public void update(Property property) {
        if (property.getHiddenVisibilities() != null) {
            for (Visibility v : property.getHiddenVisibilities()) {
                addHiddenVisibility(v);
            }
        }

        setValue(property.getValue());
        setTimestamp(property.getTimestamp());
        updateMetadata(property);
    }
}
