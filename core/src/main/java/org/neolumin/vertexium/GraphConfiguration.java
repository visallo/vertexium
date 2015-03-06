package org.neolumin.vertexium;

import org.neolumin.vertexium.id.IdGenerator;
import org.neolumin.vertexium.id.UUIDIdGenerator;
import org.neolumin.vertexium.search.DefaultSearchIndex;
import org.neolumin.vertexium.search.SearchIndex;
import org.neolumin.vertexium.util.ConfigurationUtils;

import java.util.Map;

public class GraphConfiguration {
    public static final String IDGENERATOR_PROP_PREFIX = "idgenerator";
    public static final String SEARCH_INDEX_PROP_PREFIX = "search";
    public static final String AUTO_FLUSH = "autoFlush";

    public static final String DEFAULT_IDGENERATOR = UUIDIdGenerator.class.getName();
    public static final String DEFAULT_SEARCH_INDEX = DefaultSearchIndex.class.getName();
    public static final boolean DEFAULT_AUTO_FLUSH = false;

    private final Map config;

    public GraphConfiguration(Map config) {
        this.config = config;
    }

    public void set(String key, Object value) {
        this.config.put(key, value);
    }

    public Map getConfig() {
        return config;
    }

    @SuppressWarnings("unchecked")
    public Object getConfig(String key, Object defaultValue) {
        Object o = getConfig().get(key);
        if (o == null) {
            return defaultValue;
        }
        return o;
    }

    public IdGenerator createIdGenerator() throws VertexiumException {
        return ConfigurationUtils.createProvider(this, IDGENERATOR_PROP_PREFIX, DEFAULT_IDGENERATOR);
    }

    public SearchIndex createSearchIndex() throws VertexiumException {
        return ConfigurationUtils.createProvider(this, SEARCH_INDEX_PROP_PREFIX, DEFAULT_SEARCH_INDEX);
    }

    public boolean getBoolean(String configKey, boolean defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Boolean.parseBoolean((String) obj);
        }
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        return Boolean.valueOf(obj.toString());
    }

    public double getDouble(String configKey, double defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        if (obj instanceof Double) {
            return (double) obj;
        }
        return Double.valueOf(obj.toString());
    }

    public int getInt(String configKey, int defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Integer) {
            return (int) obj;
        }
        return Integer.valueOf(obj.toString());
    }

    public long getConfigLong(String key, long defaultValue) {
        Object obj = config.get(key);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Long) {
            return (long) obj;
        }
        return Long.valueOf(obj.toString());
    }

    public String getString(String configKey, String defaultValue) {
        Object str = config.get(configKey);
        if (str == null) {
            return defaultValue;
        }
        if (str instanceof String) {
            return (String) str;
        }
        return str.toString();
    }
}
