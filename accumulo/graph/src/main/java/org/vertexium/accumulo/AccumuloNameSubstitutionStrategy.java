package org.vertexium.accumulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.Map;

public class AccumuloNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private final Cache<ByteSequence, String> inflateTextCache;

    protected AccumuloNameSubstitutionStrategy(NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
        inflateTextCache = Cache2kBuilder.of(ByteSequence.class, String.class)
            .name(AccumuloNameSubstitutionStrategy.class, "inflateTextCache-" + System.identityHashCode(this))
            .entryCapacity(10000)
            .eternal(true)
            .loader(byteSequence -> inflate(ByteSequenceUtils.toString(byteSequence)))
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
