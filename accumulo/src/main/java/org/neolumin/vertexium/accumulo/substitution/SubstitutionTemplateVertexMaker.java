package org.neolumin.vertexium.accumulo.substitution;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.accumulo.VertexMaker;

import java.util.Iterator;
import java.util.Map;

public class SubstitutionTemplateVertexMaker extends VertexMaker {
    private SubstitutionTemplate substitutionTemplate;

    public SubstitutionTemplateVertexMaker(SubstitutionAccumuloGraph substitutionAccumuloGraph, Iterator<Map.Entry<Key, Value>> next, Authorizations authorizations, SubstitutionTemplate substitutionTemplate) {
        super(substitutionAccumuloGraph, next, authorizations);
        this.substitutionTemplate = substitutionTemplate;
    }

    @Override
    protected Text getColumnFamily(Key key) {
        return inflate(key.getColumnFamily());
    }

    @Override
    protected Text getColumnQualifier(Key key) {
        return inflate(key.getColumnQualifier());
    }

    private Text inflate(Text text){
        return new Text(substitutionTemplate.inflate(text.toString()));
    }
}
