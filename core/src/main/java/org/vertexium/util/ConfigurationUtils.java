package org.vertexium.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConfigurationUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ConfigurationUtils.class);

    public static <T> T createProvider(Graph graph, GraphConfiguration config, String propPrefix, String defaultProvider) throws VertexiumException {
        String implClass = config.getString(propPrefix, defaultProvider);
        checkNotNull(implClass, "createProvider could not find " + propPrefix + " configuration item");
        return createProvider(implClass, graph, config);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProvider(String className, Graph graph, GraphConfiguration config) throws VertexiumException {
        checkNotNull(className, "className is required");
        className = className.trim();
        LOGGER.debug("creating provider '%s'", className);
        Class<Graph> graphClass = Graph.class;
        Class<GraphConfiguration> graphConfigurationClass = GraphConfiguration.class;
        try {
            Class<?> clazz = Class.forName(className);
            try {
                Constructor constructor;
                try {
                    constructor = clazz.getConstructor(graphClass);
                    return (T) constructor.newInstance(graph);
                } catch (NoSuchMethodException ignore1) {
                    try {
                        constructor = clazz.getConstructor(graphClass, graphConfigurationClass);
                        return (T) constructor.newInstance(graph, config);
                    } catch (NoSuchMethodException ignore2) {
                        try {
                            constructor = clazz.getConstructor(graphConfigurationClass);
                            return (T) constructor.newInstance(config);
                        } catch (NoSuchMethodException ignore3) {
                            try {
                                constructor = clazz.getConstructor(Map.class);
                                return (T) constructor.newInstance(config.getConfig());
                            } catch (NoSuchMethodException ignoreInner) {
                                constructor = clazz.getConstructor();
                                return (T) constructor.newInstance();
                            }
                        }
                    }
                }
            } catch (IllegalArgumentException e) {
                StringBuilder possibleMatches = new StringBuilder();
                for (Constructor<?> s : clazz.getConstructors()) {
                    possibleMatches.append(s.toGenericString());
                    possibleMatches.append(", ");
                }
                throw new VertexiumException("Invalid constructor for " + className + ". Expected <init>(" + graphConfigurationClass.getName() + "). Found: " + possibleMatches, e);
            }
        } catch (NoSuchMethodException e) {
            throw new VertexiumException("Provider must have a single argument constructor taking a " + graphConfigurationClass.getName(), e);
        } catch (ClassNotFoundException e) {
            throw new VertexiumException("No provider found with class name " + className, e);
        } catch (Exception e) {
            throw new VertexiumException(e);
        }
    }

    public static Map<String, String> loadConfig(List<String> configFileNames, String configPropertyPrefix) throws IOException {
        Map<String, String> props = loadFiles(configFileNames);
        resolvePropertyReferences(props);
        return stripPrefix(props, configPropertyPrefix);
    }

    private static Map<String, String> loadFiles(List<String> configFileNames) throws IOException {
        Properties props = new Properties();
        for (String configFileName : configFileNames) {
            File configFile = new File(configFileName);
            if (!configFile.exists()) {
                throw new RuntimeException("Could not load config file: " + configFile.getAbsolutePath());
            }

            try (InputStream in = new FileInputStream(configFile)) {
                props.load(in);
            }
        }
        return propertiesToMap(props);
    }

    private static Map<String, String> stripPrefix(Map<String, String> propsMap, String configPropertyPrefix) {
        Map<String, String> result = new HashMap<>();
        if (configPropertyPrefix == null) {
            result.putAll(propsMap);
        } else {
            for (Map.Entry<String, String> p : propsMap.entrySet()) {
                if (p.getKey().startsWith(configPropertyPrefix + ".")) {
                    result.put(p.getKey().substring((configPropertyPrefix + ".").length()), p.getValue());
                } else if (p.getKey().startsWith(configPropertyPrefix)) {
                    result.put(p.getKey().substring(configPropertyPrefix.length()), p.getValue());
                }
            }
        }

        return result;
    }

    private static Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> results = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            results.put("" + entry.getKey(), "" + entry.getValue());
        }
        return results;
    }

    private static void resolvePropertyReferences(Map<String, String> config) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String entryValue = (String) ((Map.Entry) entry).getValue();
            if (!StringUtils.isBlank(entryValue)) {
                entry.setValue(StrSubstitutor.replace(entryValue, config));
            }
        }
    }
}
