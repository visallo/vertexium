package org.vertexium.id;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.cache2k.CacheSource;

import java.util.List;
import java.util.Map;

public class SimpleNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private List<Pair<String, String>> substitutionList = Lists.newArrayList();
    private final Cache<String, String> deflateCache;
    private final Cache<String, String> inflateCache;
    public static final String SUBS_DELIM = "\u0002";

    public SimpleNameSubstitutionStrategy() {
        deflateCache = CacheBuilder
                .newCache(String.class, String.class)
                .name(SimpleNameSubstitutionStrategy.class, "deflateCache-" + System.identityHashCode(this))
                .maxSize(1000)
                .source(new CacheSource<String, String>() {
                    @Override
                    public String get(String value) throws Throwable {
                        String deflatedVal = value;
                        for (Pair<String, String> pair : substitutionList) {
                            deflatedVal = deflatedVal.replaceAll(pair.getKey(), wrap(pair.getValue()));
                        }
                        inflateCache.put(deflatedVal, value);
                        return deflatedVal;
                    }
                })
                .build();

        inflateCache = CacheBuilder
                .newCache(String.class, String.class)
                .name(SimpleNameSubstitutionStrategy.class, "inflateCache-" + System.identityHashCode(this))
                .maxSize(1000)
                .source(new CacheSource<String, String>() {
                    @Override
                    public String get(String value) throws Throwable {
                        String inflatedValue = value;
                        for (Pair<String, String> pair : substitutionList) {
                            inflatedValue = inflatedValue.replaceAll(wrap(pair.getValue()), pair.getKey());
                        }
                        deflateCache.put(inflatedValue, value);
                        return inflatedValue;
                    }
                })
                .build();
    }

    @Override
    public void setup(Map config) {
        this.setSubstitutionList(SimpleSubstitutionUtils.getSubstitutionList(config));
    }

    @Override
    public String deflate(String value) {
        return deflateCache.get(value);
    }

    @Override
    public String inflate(String value) {
        return inflateCache.get(value);
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
