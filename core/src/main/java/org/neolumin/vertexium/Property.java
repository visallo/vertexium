package org.neolumin.vertexium;

public abstract class Property implements Comparable<Property> {

    public abstract String getKey();

    public abstract String getName();

    public abstract Object getValue();

    public abstract Visibility getVisibility();

    public abstract Metadata getMetadata();

    public abstract Iterable<Visibility> getHiddenVisibilities();

    public abstract boolean isHidden(Authorizations authorizations);

    @Override
    public int compareTo(Property o) {
        int i = getName().compareTo(o.getName());
        if (i != 0) {
            return i;
        }
        i = getKey().compareTo(o.getKey());
        if (i != 0) {
            return i;
        }
        return getVisibility().compareTo(o.getVisibility());
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ getKey().hashCode() ^ getVisibility().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Property) {
            Property other = (Property) obj;
            return getName().equals(other.getName())
                    && getKey().equals(other.getKey())
                    && getVisibility().equals(other.getVisibility());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return "[" + getName() + ":" + getKey() + ":" + getVisibility() + "]";
    }
}
