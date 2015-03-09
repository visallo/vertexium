package org.neolumin.vertexium.accumulo.substitution;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.accumulo.AccumuloGraph;
import org.neolumin.vertexium.accumulo.EdgeMaker;

import java.util.Iterator;
import java.util.Map;

public class SubstitiutionTemplateEdgeMaker extends EdgeMaker {
    private SubstitutionTemplate template;

    public SubstitiutionTemplateEdgeMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> iterator, Authorizations authorizations, SubstitutionTemplate substitutionTemplate) {
        super(graph, iterator, authorizations);
        this.template = substitutionTemplate;
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
        return new Text(this.template.inflate(text.toString()));
    }
}
