package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class VertexiumSaveExtendedDataMutationNativeScriptFactory implements NativeScriptFactory {
    @Override
    public ExecutableScript newScript(Map<String, Object> params) {
        return new VertexiumSaveExtendedDataMutationNativeScript(params);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public String getName() {
        return "vertexiumSaveExtendedDataMutation";
    }
}
