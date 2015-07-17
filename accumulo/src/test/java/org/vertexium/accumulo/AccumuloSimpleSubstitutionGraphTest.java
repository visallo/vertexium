package org.vertexium.accumulo;

import com.google.common.base.Joiner;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.id.SimpleNameSubstitutionStrategy;
import org.vertexium.id.SimpleSubstitutionUtils;

import java.util.Map;

@RunWith(JUnit4.class)
public class AccumuloSimpleSubstitutionGraphTest extends AccumuloGraphTestBase {
    protected Map createConfig() {
        Map configMap = super.createConfig();
        configMap.put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.KEY_IDENTIFIER}), "k1");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "0", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), "k");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.KEY_IDENTIFIER}), "author");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "1", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), "a");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.KEY_IDENTIFIER}), "label");
        configMap.put(Joiner.on('.').join(new String[]{SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX, "2", SimpleSubstitutionUtils.VALUE_IDENTIFIER}), "l");
        return configMap;
    }

    @Override
    protected String substitutionDeflate(String str) {
        if (str.equals("author")) {
            return SimpleNameSubstitutionStrategy.SUBS_DELIM + "a" + SimpleNameSubstitutionStrategy.SUBS_DELIM;
        }
        return str;
    }
}
