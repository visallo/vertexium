package org.vertexium;

public class VertexiumTypeException extends VertexiumException {
    private final String name;
    private final Class<?> valueClass;

    public VertexiumTypeException(String name, Class<?> valueClass) {
        super(createMessage(name, valueClass));
        this.name = name;
        this.valueClass = valueClass;
    }

    private static String createMessage(String name, Class<?> valueClass) {
        return String.format("Property type not defined for property \"%s\" of type \"%s\"", name, valueClass.getName());
    }

    public String getName() {
        return name;
    }

    public Class<?> getValueClass() {
        return valueClass;
    }
}
