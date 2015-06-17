package org.vertexium.id;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class SimpleNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private List<Pair<String, String>> substitutionList = Lists.newArrayList();
    private final LoadingCache<String, String> deflateCache;
    private final LoadingCache<String, String> inflateCache;
    public static final String SUBS_DELIM = "\u0002";

    public SimpleNameSubstitutionStrategy() {
        deflateCache = CacheBuilder.newBuilder().maximumSize(1000L).build(new CacheLoader<String, String>() {
            @Override
            public String load(String value) throws Exception {
                String deflatedVal = value;
                for (Pair<String, String> pair : substitutionList) {
                    deflatedVal = deflatedVal.replaceAll(pair.getKey(), wrap(pair.getValue()));
                }
                inflateCache.put(deflatedVal, value);
                return deflatedVal;
            }
        });

        inflateCache = CacheBuilder.newBuilder().maximumSize(1000L).build(new CacheLoader<String, String>() {
            @Override
            public String load(String value) throws Exception {
                String inflatedValue = value;
                for (Pair<String, String> pair : substitutionList) {
                    inflatedValue = inflatedValue.replaceAll(wrap(pair.getValue()), pair.getKey());
                }
                deflateCache.put(inflatedValue, value);
                return inflatedValue;
            }
        });
    }

    @Override
    public void setup(Map config) {
        this.setSubstitutionList(SimpleSubstitutionUtils.getSubstitutionList(config));
    }

    @Override
    public String deflate(String value) {
        return deflateCache.getUnchecked(value);
    }

    @Override
    public String inflate(String value) {
        return inflateCache.getUnchecked(value);
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
