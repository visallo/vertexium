package org.vertexium.cypher.exceptions;

import java.util.Arrays;
import java.util.stream.Collectors;

public class VertexiumCypherTypeErrorException extends VertexiumCypherException {
    private static final long serialVersionUID = 4097761075124617864L;

    public VertexiumCypherTypeErrorException(String message) {
        super(message);
    }

    public VertexiumCypherTypeErrorException(Object value, Class... acceptedClasses) {
        super(createMessage(value, acceptedClasses));
    }

    private static String createMessage(Object value, Class[] acceptedClasses) {
        return String.format(
            "InvalidArgumentValue: expected one of [%s] found \"%s\"",
            Arrays.stream(acceptedClasses)
                .map((c) -> c == null ? "null" : c.getName())
                .collect(Collectors.joining(", ")),
            value == null ? "null" : value.getClass().getName()
        );
    }

    public static void assertType(Object value, Class... acceptedClasses) {
        for (Class acceptedClass : acceptedClasses) {
            if (acceptedClass == null) {
                if (value == null) {
                    return;
                }
            } else if (acceptedClass.isInstance(value)) {
                return;
            }
        }
        throw new VertexiumCypherTypeErrorException(value, acceptedClasses);
    }
}
