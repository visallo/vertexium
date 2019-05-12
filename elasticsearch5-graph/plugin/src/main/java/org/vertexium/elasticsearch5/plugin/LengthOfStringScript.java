package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Map;

public class LengthOfStringScript extends VertexiumSearchScript {
    private final List<String> fieldNames;
    private final SortOrder sortOrder;

    @SuppressWarnings("unchecked")
    public LengthOfStringScript(SearchLookup lookup, Map<String, Object> vars) {
        this(
            lookup,
            (List<String>) vars.get("fieldNames"),
            SortOrder.fromString((String) vars.get("direction"))
        );
    }

    public LengthOfStringScript(SearchLookup lookup, List<String> fieldNames, SortOrder sortOrder) {
        super(lookup);
        this.fieldNames = fieldNames;
        this.sortOrder = sortOrder;
    }

    @Override
    protected double runAsDouble(LeafSearchLookup leafSearchLookup) {
        LeafDocLookup doc = leafSearchLookup.doc();
        int length = (sortOrder == SortOrder.ASC) ? Integer.MAX_VALUE : 0;

        for (String fieldName : fieldNames) {
            if (!doc.containsKey(fieldName)) {
                continue;
            }

            List<?> values = doc.get(fieldName).getValues();
            for (Object value : values) {
                int valueLength = value.toString().length();
                if (sortOrder == SortOrder.ASC) {
                    length = (valueLength < length) ? valueLength : length;
                } else {
                    length = (valueLength > length) ? valueLength : length;
                }
            }
        }
        if (sortOrder == SortOrder.DESC) {
            length = -length;
        }
        return length;
    }
}
