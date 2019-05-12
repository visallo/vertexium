package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SortScript extends VertexiumSearchScript {
    private final List<String> fieldNames;
    private final String dataType;
    private final SortOrder esOrder;

    @SuppressWarnings("unchecked")
    public SortScript(SearchLookup lookup, Map<String, Object> vars) {
        this(
            lookup,
            (List<String>) vars.get("fieldNames"),
            (String) vars.get("dataType"),
            SortOrder.fromString((String) vars.get("esOrder"))
        );
    }

    public SortScript(SearchLookup lookup, List<String> fieldNames, String dataType, SortOrder esOrder) {
        super(lookup);
        this.fieldNames = fieldNames;
        this.dataType = dataType;
        this.esOrder = esOrder;
    }

    @Override
    protected Object run(LeafSearchLookup leafSearchLookup) {
        LeafDocLookup doc = leafSearchLookup.doc();
        List<? extends Comparable> fieldValues = getFieldValues(doc);
        if (esOrder == SortOrder.ASC) {
            Collections.sort(fieldValues);
        } else {
            Collections.sort(fieldValues, Collections.reverseOrder());
        }
        if (dataType.equals(String.class.getName())) {
            return fieldValues;
        } else {
            if (fieldValues.size() > 0) {
                return fieldValues.get(0);
            } else {
                return esOrder == SortOrder.ASC ? Long.MAX_VALUE : Long.MIN_VALUE;
            }
        }
    }

    @Override
    protected long runAsLong(LeafSearchLookup leafSearchLookup) {
        return ((Number) run(leafSearchLookup)).longValue();
    }

    @Override
    protected double runAsDouble(LeafSearchLookup leafSearchLookup) {
        return ((Number) run(leafSearchLookup)).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable> List<T> getFieldValues(LeafDocLookup doc) {
        List<T> fieldValues = new ArrayList<>();
        for (String fieldName : fieldNames) {
            if (doc.containsKey(fieldName)) {
                List<T> values = (List<T>) doc.get(fieldName).getValues();
                fieldValues.addAll(values);
            }
        }
        return fieldValues;
    }
}
