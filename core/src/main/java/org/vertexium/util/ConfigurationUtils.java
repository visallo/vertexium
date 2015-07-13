package org.vertexium.util;

import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;

import java.lang.reflect.Constructor;
import java.util.Map;

import static org.vertexium.util.Preconditions.checkNotNull;

public class ConfigurationUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ConfigurationUtils.class);

    public static <T> T createProvider(GraphConfiguration config, String propPrefix, String defaultProvider) throws VertexiumException {
        String implClass = config.getString(propPrefix, defaultProvider);
        checkNotNull(implClass, "createProvider could not find " + propPrefix + " configuration item");
        return createProvider(implClass, config);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProvider(String className, GraphConfiguration config) throws VertexiumException {
        checkNotNull(className, "className is required");
        className = className.trim();
        LOGGER.debug("creating provider '%s'", className);
        Class<GraphConfiguration> constructorParameterClass = GraphConfiguration.class;
        try {
            Class<?> clazz = Class.forName(className);
            try {
                Constructor constructor;
                try {
                    constructor = clazz.getConstructor(constructorParameterClass);
                    return (T) constructor.newInstance(config);
                } catch (NoSuchMethodException ignore) {
                    try {
                        constructor = clazz.getConstructor(Map.class);
                        return (T) constructor.newInstance(config.getConfig());
                    } catch (NoSuchMethodException ignoreInner) {
                        constructor = clazz.getConstructor();
                        return (T) constructor.newInstance();
                    }
                }
            } catch (IllegalArgumentException e) {
                StringBuilder possibleMatches = new StringBuilder();
                for (Constructor<?> s : clazz.getConstructors()) {
                    possibleMatches.append(s.toGenericString());
                    possibleMatches.append(", ");
                }
                throw new VertexiumException("Invalid constructor for " + className + ". Expected <init>(" + constructorParameterClass.getName() + "). Found: " + possibleMatches, e);
            }
        } catch (NoSuchMethodException e) {
            throw new VertexiumException("Provider must have a single argument constructor taking a " + constructorParameterClass.getName(), e);
        } catch (ClassNotFoundException e) {
            throw new VertexiumException("No provider found with class name " + className, e);
        } catch (Exception e) {
            throw new VertexiumException(e);
        }
    }
}
