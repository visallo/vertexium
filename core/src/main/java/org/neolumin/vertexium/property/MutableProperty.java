package org.neolumin.vertexium.property;

import org.neolumin.vertexium.Metadata;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.Visibility;

public abstract class MutableProperty extends Property {
    public abstract void setValue(Object value);

    public abstract void setVisibility(Visibility visibility);

    public abstract void addHiddenVisibility(Visibility visibility);

    public abstract void removeHiddenVisibility(Visibility visibility);

    protected abstract void addMetadata(String key, Object value, Visibility visibility);

    protected abstract void removeMetadata(String key, Visibility visibility);

    public void update(Property property) {
        if (property.getHiddenVisibilities() != null) {
            for (Visibility v : property.getHiddenVisibilities()) {
                addHiddenVisibility(v);
            }
        }

        setValue(property.getValue());

        for (Metadata.Entry m : property.getMetadata().entrySet()) {
            if (m.getValue() == null) {
                removeMetadata(m.getKey(), m.getVisibility());
            } else {
                addMetadata(m.getKey(), m.getValue(), m.getVisibility());
            }
        }
    }
}
