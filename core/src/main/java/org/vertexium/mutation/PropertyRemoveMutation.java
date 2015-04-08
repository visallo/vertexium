package org.vertexium.mutation;

import org.vertexium.Visibility;

public abstract class PropertyRemoveMutation implements Comparable<PropertyRemoveMutation> {
    public abstract String getKey();

    public abstract String getName();

    public abstract Visibility getVisibility();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof PropertyRemoveMutation)) {
            return false;
        }

        PropertyRemoveMutation that = (PropertyRemoveMutation) o;

        if (getKey() != null ? !getKey().equals(that.getKey()) : that.getKey() != null) {
            return false;
        }
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        if (getVisibility() != null ? !getVisibility().equals(that.getVisibility()) : that.getVisibility() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int compareTo(PropertyRemoveMutation that) {
        if (this == that) {
            return 0;
        }
        if (that == null) {
            return -1;
        }

        if (getKey() != null && that.getKey() == null) {
            return -1;
        }
        if (getKey() == null && that.getKey() != null) {
            return 1;
        }
        if (getKey() != null) {
            int result = getKey().compareTo(that.getKey());
            if (result != 0) {
                return result;
            }
        }

        if (getName() != null && that.getName() == null) {
            return -1;
        }
        if (getName() == null && that.getName() != null) {
            return 1;
        }
        if (getName() != null) {
            int result = getName().compareTo(that.getName());
            if (result != 0) {
                return result;
            }
        }

        if (getVisibility() != null && that.getVisibility() == null) {
            return -1;
        }
        if (getVisibility() == null && that.getVisibility() != null) {
            return 1;
        }
        if (getVisibility() != null) {
            int result = getVisibility().compareTo(that.getVisibility());
            if (result != 0) {
                return result;
            }
        }

        return 0;
    }

    @Override
    public int hashCode() {
        int result = getKey() != null ? getKey().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getVisibility() != null ? getVisibility().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PropertyRemoveMutation{" +
                "key='" + getKey() + '\'' +
                ", name='" + getName() + '\'' +
                ", visibility=" + getVisibility() +
                '}';
    }
}
