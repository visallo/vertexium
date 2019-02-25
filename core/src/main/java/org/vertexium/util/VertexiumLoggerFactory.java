package org.vertexium.util;

import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class VertexiumLoggerFactory {
    private static final Map<String, VertexiumLogger> logMap = new HashMap<>();

    public static VertexiumLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static VertexiumLogger getLogger(String name) {
        synchronized (logMap) {
            VertexiumLogger vertexiumLogger = logMap.get(name);
            if (vertexiumLogger != null) {
                return vertexiumLogger;
            }
            vertexiumLogger = new VertexiumLogger(LoggerFactory.getLogger(name));
            logMap.put(name, vertexiumLogger);
            return vertexiumLogger;
        }
    }

    public static VertexiumLogger getMutationLogger(Class clazz) {
        return getLogger(clazz.getName() + ".MUTATION");
    }

    public static VertexiumLogger getQueryLogger(Class clazz) {
        return getLogger(clazz.getName() + ".QUERY");
    }
}
