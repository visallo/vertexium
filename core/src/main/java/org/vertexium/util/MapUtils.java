package org.vertexium.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {
    @SuppressWarnings("unchecked")
    public static Map getAllWithPrefix(Map map, String prefix) {
        Map result = new HashMap();
        for (Object e : map.entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                String keySub = key.substring(prefix.length());
                if (keySub.startsWith(".")) {
                    keySub = keySub.substring(1);
                }
                result.put(keySub, entry.getValue());
            }
        }
        return result;
    }
}
