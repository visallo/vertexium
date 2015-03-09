package org.neolumin.vertexium.accumulo.substitution;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neolumin.vertexium.accumulo.AccumuloGraphConfiguration;
import org.neolumin.vertexium.util.IterableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SubstitutionAccumuloGraphConfiguration extends AccumuloGraphConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubstitutionAccumuloGraphConfiguration.class);
    public static final String SUBSTITUTION_MAP_PREFIX = "substitution";
    public static final String KEY_IDENTIFIER = "key";
    public static final String VALUE_IDENTIFIER = "value";

    public SubstitutionAccumuloGraphConfiguration(Map config) {
        super(config);
    }

    public List<Pair<String, String>> getSubstitionList(){
        Map<String, MutablePair<String, String>> substitutionMap = Maps.newHashMap();

        //parse the config arguments
        for(Object objKey : getConfig().keySet()){
            String key = objKey.toString();
            if(key.startsWith(SUBSTITUTION_MAP_PREFIX)){
                List<String> parts = Lists.newArrayList(IterableUtils.toList(Splitter.on('.').split(key)));
                String pairKey = parts.get(parts.size() - 2);
                String valueType = parts.get(parts.size() - 1);

                if(!substitutionMap.containsKey(pairKey)){
                    substitutionMap.put(pairKey, new MutablePair<String, String>());
                }

                MutablePair<String, String> pair = substitutionMap.get(pairKey);

                if(KEY_IDENTIFIER.equals(valueType)){
                    pair.setLeft(getString(key, null));
                }
                else if(VALUE_IDENTIFIER.equals(valueType)){
                    pair.setValue(getString(key, null));
                }
            }
        }

        //order is important, so create order by the pairKey that was in the config.  eg: substitution.0.key is before substitution.1.key so it is evaluated in that order
        List<String> keys = Lists.newArrayList(substitutionMap.keySet());
        Collections.sort(keys);

        List<Pair<String, String>> finalMap = Lists.newArrayList();
        for(String key : keys){
            Pair<String, String> pair = substitutionMap.get(key);
            finalMap.add(pair);
            LOGGER.info(String.format("Using substitution %s -> %s", pair.getKey(), pair.getValue()));
        }

        return finalMap;
    }
}
