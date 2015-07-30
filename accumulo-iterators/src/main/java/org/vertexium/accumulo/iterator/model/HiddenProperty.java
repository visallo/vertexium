package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class HiddenProperty {
    private final String key;
    private final String name;
    private final String visibility;
    private final Text hiddenVisibility;

    public HiddenProperty(String key, String name, String visibility, Text hiddenVisibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.hiddenVisibility = hiddenVisibility;
    }

    public boolean matches(String propertyKey, String propertyName, String visibility) {
        return propertyKey.equals(this.key)
                && propertyName.equals(this.name)
                && visibility.equals(this.visibility);
    }

    public Text getHiddenVisibility() {
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

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) {
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
