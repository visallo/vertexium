package org.vertexium.blueprints;

import org.vertexium.VertexiumException;
import org.vertexium.util.MapUtils;

import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

public abstract class VertexiumBlueprintsFactory {
    public static final String STORAGE_CONFIG_PREFIX = "storage";

    public static VertexiumBlueprintsGraph open(String configFileName) throws Exception {
        try {
            try (FileInputStream in = new FileInputStream(configFileName)) {
                Properties properties = new Properties();
                properties.load(in);

                String storageFactoryClassName = properties.getProperty(STORAGE_CONFIG_PREFIX);
                Map storageConfig = MapUtils.getAllWithPrefix(properties, STORAGE_CONFIG_PREFIX);
                return createFactory(storageFactoryClassName).createGraph(storageConfig);
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            throw ex;
        }
    }

    protected abstract VertexiumBlueprintsGraph createGraph(Map config);

    private static VertexiumBlueprintsFactory createFactory(String factoryClassName) {
        try {
            Class factoryClass = Class.forName(factoryClassName);
            Constructor constructor = factoryClass.getConstructor();
            return (VertexiumBlueprintsFactory) constructor.newInstance();
        } catch (Exception ex) {
            throw new VertexiumException("Could not create factory: " + factoryClassName, ex);
        }
    }
}
