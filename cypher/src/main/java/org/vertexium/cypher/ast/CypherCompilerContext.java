package org.vertexium.cypher.ast;

import org.vertexium.cypher.functions.CypherFunction;

import java.util.HashMap;
import java.util.Map;

public class CypherCompilerContext {
    private final Map<String, CypherFunction> functions;

    public CypherCompilerContext() {
        this(new HashMap<>());
    }

    public CypherCompilerContext(Map<String, CypherFunction> functions) {
        this.functions = functions;
    }

    public CypherFunction getFunction(String functionName) {
        return functions.get(functionName.toLowerCase());
    }
}
