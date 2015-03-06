package org.neolumin.vertexium.property;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Metadata;
import org.neolumin.vertexium.Visibility;

import java.util.HashSet;
import java.util.Set;

public class MutablePropertyImpl extends MutableProperty {
    private final String key;
    private final String name;
    private Set<Visibility> hiddenVisibilities;
    private Object value;
    private Visibility visibility;
    private final Metadata metadata;

    public MutablePropertyImpl(String key, String name, Object value, Metadata metadata, Set<Visibility> hiddenVisibilities, Visibility visibility) {
        if (metadata == null) {
            metadata = new Metadata();
        }

        this.key = key;
        this.name = name;
        this.value = value;
        this.metadata = metadata;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return this.hiddenVisibilities;
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        if (hiddenVisibilities != null) {
            for (Visibility v : getHiddenVisibilities()) {
                if (authorizations.canRead(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public void addHiddenVisibility(Visibility visibility) {
        if (this.hiddenVisibilities == null) {
            this.hiddenVisibilities = new HashSet<Visibility>();
        }
        this.hiddenVisibilities.add(visibility);
    }

    @Override
    public void removeHiddenVisibility(Visibility visibility) {
        if (this.hiddenVisibilities == null) {
            this.hiddenVisibilities = new HashSet<Visibility>();
        }
        this.hiddenVisibilities.remove(visibility);
    }

    @Override
    protected void addMetadata(String key, Object value, Visibility visibility) {
        this.metadata.add(key, value, visibility);
    }

    @Override
    protected void removeMetadata(String key, Visibility visibility) {
        this.metadata.remove(key, visibility);
    }
}
