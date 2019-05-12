package org.vertexium.id;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleSubstitutionUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SimpleSubstitutionUtils.class);
    public static final String SUBSTITUTION_MAP_PREFIX = "substitution";
    public static final String KEY_IDENTIFIER = "key";
    public static final String VALUE_IDENTIFIER = "value";

    public static List<Pair<String, String>> getSubstitutionList(Map configuration) {
        Map<String, MutablePair<String, String>> substitutionMap = Maps.newHashMap();

        //parse the config arguments
        for (Object objKey : configuration.keySet()) {
            String key = objKey.toString();
            if (key.startsWith(SUBSTITUTION_MAP_PREFIX + ".")) {
                List<String> parts = Lists.newArrayList(IterableUtils.toList(Splitter.on('.').split(key)));
                String pairKey = parts.get(parts.size() - 2);
                String valueType = parts.get(parts.size() - 1);

                if (!substitutionMap.containsKey(pairKey)) {
                    substitutionMap.put(pairKey, new MutablePair<>());
                }

                MutablePair<String, String> pair = substitutionMap.get(pairKey);

                if (KEY_IDENTIFIER.equals(valueType)) {
                    pair.setLeft(configuration.get(key).toString());
                } else if (VALUE_IDENTIFIER.equals(valueType)) {
                    pair.setValue(configuration.get(key).toString());
                }
            }
        }

        //order is important, so create order by the pairKey that was in the config.  eg: substitution.0.key is before substitution.1.key so it is evaluated in that order
        List<String> keys = Lists.newArrayList(substitutionMap.keySet());
        Collections.sort(keys);

        List<Pair<String, String>> finalMap = Lists.newArrayList();
        for (String key : keys) {
            Pair<String, String> pair = substitutionMap.get(key);
            finalMap.add(pair);
            LOGGER.info("Using substitution %s -> %s", pair.getKey(), pair.getValue());
        }

        return finalMap;
    }
}
