package org.vertexium;

import java.io.Serializable;
import java.util.Set;

public class Visibility implements Serializable, Comparable<Visibility> {
    static final long serialVersionUID = 42L;
    public static final Visibility EMPTY = new Visibility("");
    private final String visibilityString;

    public Visibility(String visibilityString) {
        this.visibilityString = visibilityString;
    }

    public String getVisibilityString() {
        return visibilityString;
    }

    @Override
    public String toString() {
        return getVisibilityString();
    }

    @Override
    public int hashCode() {
        return getVisibilityString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Visibility) {
            Visibility objVisibility = (Visibility) obj;
            return getVisibilityString().equals(objVisibility.getVisibilityString());
        }
        return super.equals(obj);
    }

    @Override
    public int compareTo(Visibility o) {
        return getVisibilityString().compareTo(o.getVisibilityString());
    }

    public static Visibility and(Set<String> visibilityStrings) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String visibilityString : visibilityStrings) {
            if (visibilityString == null || visibilityString.length() == 0) {
                continue;
            }
            if (!first) {
                result.append('&');
            }
            result.append('(');
            result.append(visibilityString);
            result.append(')');
            first = false;
        }
        return new Visibility(result.toString());
    }
}
