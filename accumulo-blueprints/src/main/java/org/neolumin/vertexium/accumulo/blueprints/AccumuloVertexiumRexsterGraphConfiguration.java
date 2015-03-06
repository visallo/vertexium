package org.neolumin.vertexium.accumulo.blueprints;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.neolumin.vertexium.blueprints.VertexiumRexsterGraphConfiguration;
import org.neolumin.vertexium.util.ApacheConfigurationUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccumuloVertexiumRexsterGraphConfiguration extends VertexiumRexsterGraphConfiguration {
    @Override
    public Graph configureGraphInstance(Configuration configuration) throws GraphConfigurationException {
        Map configurationMap = ApacheConfigurationUtils.toMap(configuration);
        Map convertedConfigurationMap = new HashMap();
        for (Object key : configurationMap.keySet()) {
            Object convertedKey = key.toString().replaceAll("-", ".");
            Object convertedValue = configurationMap.get(key);
            if (convertedValue instanceof List) {
                convertedValue = join((Iterable) convertedValue);
            }
            convertedConfigurationMap.put(convertedKey, convertedValue);
        }
        for (Object key : convertedConfigurationMap.keySet()) {
            System.out.println(key + " = " + convertedConfigurationMap.get(key));
        }
        return new AccumuloVertexiumBlueprintsGraphFactory().createGraph(convertedConfigurationMap);
    }

    private String join(Iterable value) {
        Iterator valueIterator = value.iterator();
        StringBuilder result = new StringBuilder();
        boolean first = true;
        while (valueIterator.hasNext()) {
            if (!first) {
                result.append(",");
            }
            Object v = valueIterator.next();
            result.append(v.toString());
            first = false;
        }
        return result.toString();
    }
}
