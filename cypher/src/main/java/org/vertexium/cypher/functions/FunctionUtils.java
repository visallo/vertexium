package org.vertexium.cypher.functions;

import org.vertexium.cypher.exceptions.VertexiumCypherArgumentErrorException;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FunctionUtils {
    public static void assertArgumentCount(Object[] arguments, int... expectedCounts) {
        for (int count : expectedCounts) {
            if (arguments.length == count) {
                return;
            }
        }

        throw new VertexiumCypherArgumentErrorException(String.format(
            "Unexpected number of arguments. Expected %s, found %d",
            Arrays.stream(expectedCounts).mapToObj(Integer::toString).collect(Collectors.joining(", ")),
            arguments.length
        ));
    }
}
