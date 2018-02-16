package org.vertexium.accumulo;

import com.google.common.base.Joiner;
import org.junit.ClassRule;
import org.vertexium.id.SimpleNameSubstitutionStrategy;

import java.util.HashMap;

import static org.vertexium.id.SimpleSubstitutionUtils.*;

public class AccumuloSimpleSubstitutionGraphTest extends AccumuloGraphTestBase {

    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource(new HashMap<String, String>() {{
        put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", KEY_IDENTIFIER}), "k1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", VALUE_IDENTIFIER}), "k");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", KEY_IDENTIFIER}), "author");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", VALUE_IDENTIFIER}), "a");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", KEY_IDENTIFIER}), "label");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", VALUE_IDENTIFIER}), "l");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", KEY_IDENTIFIER}), LABEL_LABEL1);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", VALUE_IDENTIFIER}), "l1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "4", KEY_IDENTIFIER}), LABEL_LABEL2);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "4", VALUE_IDENTIFIER}), "l2");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "5", KEY_IDENTIFIER}), LABEL_LABEL3);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "5", VALUE_IDENTIFIER}), "l3");
    }});

    @Override
    public AccumuloResource getAccumuloResource() {
        return accumuloResource;
    }

    @Override
    protected String substitutionDeflate(String str) {
        if (str.equals("author")) {
            return SimpleNameSubstitutionStrategy.SUBS_DELIM + "a" + SimpleNameSubstitutionStrategy.SUBS_DELIM;
        }
        return str;
    }
}
