package org.vertexium.id;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class SimpleNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private List<Pair<String, String>> substitutionList = Lists.newArrayList();
    private final Cache<String, String> deflateCache = CacheBuilder.newBuilder().maximumSize(1000L).build();
    private final Cache<String, String> inflateCache = CacheBuilder.newBuilder().maximumSize(1000L).build();
    public static final String SUBS_DELIM = "\u0002";

    @Override
    public String deflate(String value) {
        String cachedDeflatedValue = deflateCache.getIfPresent(value);

        if (cachedDeflatedValue != null) {
            return cachedDeflatedValue;
        }

        String deflatedVal = value;

        for (Pair<String, String> pair : this.substitutionList) {
            deflatedVal = deflatedVal.replaceAll(pair.getKey(), wrap(pair.getValue()));
        }

        deflateCache.put(value, deflatedVal);
        inflateCache.put(deflatedVal, value);
        return deflatedVal;
    }

    @Override
    public String inflate(String value) {
        String cachedInflatedValue = inflateCache.getIfPresent(SUBS_DELIM + value);

        if (cachedInflatedValue != null) {
            return cachedInflatedValue;
        }

        String inflatedValue = value;

        for (Pair<String, String> pair : this.substitutionList) {
            inflatedValue = inflatedValue.replaceAll(wrap(pair.getValue()), pair.getKey());
        }

        inflateCache.put(value, inflatedValue);
        deflateCache.put(inflatedValue, value);
        return inflatedValue;
    }

    public static String wrap(String str) {
        return SUBS_DELIM + str + SUBS_DELIM;
    }

    public void setSubstitutionList(List<Pair<String, String>> substitutionList) {
        this.substitutionList = substitutionList;

        for (Pair<String, String> pair : this.substitutionList) {
            deflateCache.put(pair.getKey(), wrap(pair.getValue()));
            inflateCache.put(wrap(pair.getValue()), pair.getKey());
        }
    }
}
