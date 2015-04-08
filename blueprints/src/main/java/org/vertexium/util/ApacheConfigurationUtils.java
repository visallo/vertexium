package org.vertexium.util;

import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ApacheConfigurationUtils {
    public static Map toMap(Configuration configuration) {
        Map result = new HashMap();
        Iterator keys = configuration.getKeys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            result.put(key, configuration.getProperty(key));
        }
        return result;
    }
}
