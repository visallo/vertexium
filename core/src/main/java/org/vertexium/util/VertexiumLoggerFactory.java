package org.vertexium.util;

import org.slf4j.LoggerFactory;
import org.vertexium.Graph;

import java.util.HashMap;
import java.util.Map;

public class VertexiumLoggerFactory {
    private static final Map<String, VertexiumLogger> logMap = new HashMap<>();

    public static VertexiumLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static VertexiumLogger getLogger(String name) {
        synchronized (logMap) {
            VertexiumLogger visalloLogger = logMap.get(name);
            if (visalloLogger != null) {
                return visalloLogger;
            }
            visalloLogger = new VertexiumLogger(LoggerFactory.getLogger(name));
            logMap.put(name, visalloLogger);
            return visalloLogger;
        }
    }

    public static VertexiumLogger getMutationLogger(Class clazz) {
        return getLogger(clazz.getName() + ".MUTATION");
    }

    public static VertexiumLogger getQueryLogger(Class clazz) {
        return getLogger(clazz.getName() + ".QUERY");
    }
}
