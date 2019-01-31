package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;

import java.util.Objects;

public class HiddenProperty {
    private final ByteSequence key;
    private final ByteSequence name;
    private final ByteSequence visibility;
    private final ByteSequence hiddenVisibility;

    public HiddenProperty(ByteSequence key, ByteSequence name, ByteSequence visibility, ByteSequence hiddenVisibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.hiddenVisibility = hiddenVisibility;
    }

    public boolean matches(ByteSequence propertyKey, ByteSequence propertyName, ByteSequence visibility) {
        return propertyKey.equals(this.key)
                && propertyName.equals(this.name)
                && visibility.equals(this.visibility);
    }

    public ByteSequence getHiddenVisibility() {
        return hiddenVisibility;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiddenProperty that = (HiddenProperty) o;

        if (!Objects.equals(key, that.key)) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (!Objects.equals(visibility, that.visibility)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HiddenProperty{" +
                "key='" + key + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                '}';
    }
}
