package org.vertexium.accumulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

import java.util.Map;

public class AccumuloNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private final NameSubstitutionStrategy nameSubstitutionStrategy;

    protected AccumuloNameSubstitutionStrategy(NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
    }

    @Override
    public void setup(Map config) {
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
        return inflate(ByteSequenceUtils.toString(text));
    }

    public static AccumuloNameSubstitutionStrategy create(NameSubstitutionStrategy nameSubstitutionStrategy) {
        if (nameSubstitutionStrategy instanceof AccumuloNameSubstitutionStrategy) {
            return (AccumuloNameSubstitutionStrategy) nameSubstitutionStrategy;
        }
        return new AccumuloNameSubstitutionStrategy(nameSubstitutionStrategy);
    }
}
