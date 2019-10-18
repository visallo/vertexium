package org.vertexium.accumulo;

import com.google.protobuf.ByteString;
import org.apache.accumulo.core.data.ByteSequence;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.Map;

public class AccumuloNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private final Cache<ByteSequence, String> inflateTextCache;

    protected AccumuloNameSubstitutionStrategy(NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
        inflateTextCache = CacheBuilder
            .newCache(ByteSequence.class, String.class)
            .name(AccumuloNameSubstitutionStrategy.class, "inflateTextCache-" + System.identityHashCode(this))
            .maxSize(10000)
            .source(byteSequence -> inflate(ByteSequenceUtils.toString(byteSequence)))
            .build();
    }

    @Override
    public void setup(Map config) {
        return;
    }

    @Override
    public String deflate(String value) {
        return this.nameSubstitutionStrategy.deflate(value);
    }

    @Override
    public String inflate(String value) {
        return this.nameSubstitutionStrategy.inflate(value);
    }

    public String inflate(ByteString value) {
        return this.nameSubstitutionStrategy.inflate(value.toStringUtf8());
    }

    public String inflate(ByteSequence text) {
        if (text == null) {
            return null;
        }
        if (this.nameSubstitutionStrategy instanceof IdentityNameSubstitutionStrategy) {
            return text.toString();
        }
        return inflateTextCache.get(text);
    }

    public static AccumuloNameSubstitutionStrategy create(NameSubstitutionStrategy nameSubstitutionStrategy) {
        if (nameSubstitutionStrategy instanceof AccumuloNameSubstitutionStrategy) {
            return (AccumuloNameSubstitutionStrategy) nameSubstitutionStrategy;
        }
        return new AccumuloNameSubstitutionStrategy(nameSubstitutionStrategy);
    }
}
