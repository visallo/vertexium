package org.vertexium.accumulo;

import org.apache.hadoop.io.Text;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

public class AccumuloNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private final NameSubstitutionStrategy nameSubstitutionStrategy;

    protected AccumuloNameSubstitutionStrategy(NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
    }

    @Override
    public String deflate(String value) {
        return this.nameSubstitutionStrategy.deflate(value);
    }

    @Override
    public String inflate(String value) {
        return this.nameSubstitutionStrategy.inflate(value);
    }

    public Text inflate(Text text) {
        if (this.nameSubstitutionStrategy instanceof IdentityNameSubstitutionStrategy) {
            return text;
        }
        return new Text(inflate(text.toString()));
    }

    public static AccumuloNameSubstitutionStrategy create(NameSubstitutionStrategy nameSubstitutionStrategy) {
        if (nameSubstitutionStrategy instanceof AccumuloNameSubstitutionStrategy) {
            return (AccumuloNameSubstitutionStrategy) nameSubstitutionStrategy;
        }
        return new AccumuloNameSubstitutionStrategy(nameSubstitutionStrategy);
    }
}
