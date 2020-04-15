package org.vertexium.metric;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StackTraceTracker {
    private final Set<StackTraceItem> roots = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public void addStackTrace() {
        addStackTrace(Thread.currentThread().getStackTrace());
    }

    public void addStackTrace(StackTraceElement[] stackTraceElements) {
        Set<StackTraceItem> parents = roots;
        for (int i = stackTraceElements.length - 1; i >= 0; i--) {
            StackTraceElement stackTraceElement = stackTraceElements[i];
            StackTraceItem item = addItem(parents, stackTraceElement);
            item.count++;
            parents = item.children;
        }
    }

    private StackTraceItem addItem(Set<StackTraceItem> parents, StackTraceElement stackTraceElement) {
        StackTraceItem item = getItem(parents, stackTraceElement);
        if (item == null) {
            item = new StackTraceItem(stackTraceElement);
            parents.add(item);
        }
        return item;
    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    private StackTraceItem getItem(Set<StackTraceItem> parents, StackTraceElement stackTraceElement) {
        for (StackTraceItem item : parents) {
            if (item.equals(stackTraceElement)) {
                return item;
            }
        }
        return null;
    }

    public Set<StackTraceItem> getRoots() {
        return roots;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        toString(result, "", getRoots());
        return result.toString();
    }

    private void toString(StringBuilder result, String indent, Set<StackTraceItem> items) {
        for (StackTraceItem item : items) {
            result.append(indent);
            result.append(item.toString());
            result.append("\n");
            toString(result, indent + "  ", item.children);
        }
    }

    public void reset() {
        roots.clear();
    }

    public static class StackTraceItem {
        private final Set<StackTraceItem> children = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final StackTraceElement stackTraceElement;
        private int count;

        public StackTraceItem(StackTraceElement stackTraceElement) {
            this.stackTraceElement = stackTraceElement;
        }

        @Override
        public String toString() {
            return String.format("%s (count=%d)", stackTraceElement, count);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof StackTraceElement) {
                return stackTraceElement.equals(o);
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StackTraceItem that = (StackTraceItem) o;
            return stackTraceElement.equals(that.stackTraceElement);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stackTraceElement);
        }

        public Set<StackTraceItem> getChildren() {
            return children;
        }

        public StackTraceElement getStackTraceElement() {
            return stackTraceElement;
        }

        public int getCount() {
            return count;
        }
    }
}
