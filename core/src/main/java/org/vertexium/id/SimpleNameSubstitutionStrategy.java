package org.vertexium.id;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.cache2k.CacheSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private List<DeflateItem> deflateSubstitutionList = new ArrayList<>();
    private final Cache<String, String> deflateCache;
    private long deflateCalls;
    private long deflateCacheMisses;
    private List<InflateItem> inflateSubstitutionList = new ArrayList<>();
    private final Cache<String, String> inflateCache;
    private long inflateCalls;
    private long inflateCacheMisses;
    public static final String SUBS_DELIM = "\u0002";

    public SimpleNameSubstitutionStrategy() {
        deflateCache = CacheBuilder
            .newCache(String.class, String.class)
            .name(SimpleNameSubstitutionStrategy.class, "deflateCache-" + System.identityHashCode(this))
            .maxSize(10000)
            .source(new DeflateCacheSource())
            .build();

        inflateCache = CacheBuilder
            .newCache(String.class, String.class)
            .name(SimpleNameSubstitutionStrategy.class, "inflateCache-" + System.identityHashCode(this))
            .maxSize(10000)
            .source(new InflateCacheSource())
            .build();
    }

    @Override
    public void setup(Map config) {
        this.setSubstitutionList(SimpleSubstitutionUtils.getSubstitutionList(config));
    }

    @Override
    public String deflate(String value) {
        deflateCalls++;
        return deflateCache.get(value);
    }

    @Override
    public String inflate(String value) {
        inflateCalls++;
        return inflateCache.get(value);
    }

    public static String wrap(String str) {
        return SUBS_DELIM + str + SUBS_DELIM;
    }

    public void setSubstitutionList(List<Pair<String, String>> substitutionList) {
        this.inflateSubstitutionList.clear();
        this.deflateSubstitutionList.clear();
        for (Pair<String, String> pair : substitutionList) {
            this.inflateSubstitutionList.add(new InflateItem(wrap(pair.getValue()), pair.getKey()));
            this.deflateSubstitutionList.add(new DeflateItem(pair.getKey(), wrap(pair.getValue())));
        }
    }

    public long getDeflateCalls() {
        return deflateCalls;
    }

    public long getDeflateCacheMisses() {
        return deflateCacheMisses;
    }

    public long getInflateCalls() {
        return inflateCalls;
    }

    public long getInflateCacheMisses() {
        return inflateCacheMisses;
    }

    private static class InflateItem {
        private final String pattern;
        private final String replacement;

        public InflateItem(String pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public String inflate(String value) {
            return StringUtils.replace(value, pattern, replacement);
        }
    }

    private class InflateCacheSource implements CacheSource<String, String> {
        @Override
        public String get(String value) {
            inflateCacheMisses++;
            String inflatedValue = value;
            for (InflateItem inflateItem : inflateSubstitutionList) {
                inflatedValue = inflateItem.inflate(inflatedValue);
            }
            return inflatedValue;
        }
    }

    private static class DeflateItem {
        private final String pattern;
        private final String replacement;

        public DeflateItem(String pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public String deflate(String value) {
            return StringUtils.replace(value, pattern, replacement);
        }
    }

    private class DeflateCacheSource implements CacheSource<String, String> {
        @Override
        public String get(String value) {
            deflateCacheMisses++;
            String deflatedVal = value;
            for (DeflateItem deflateItem : deflateSubstitutionList) {
                deflatedVal = deflateItem.deflate(deflatedVal);
            }
            return deflatedVal;
        }
    }
}
