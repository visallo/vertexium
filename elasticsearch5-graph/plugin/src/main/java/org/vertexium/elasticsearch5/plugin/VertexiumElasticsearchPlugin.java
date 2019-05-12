package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.vertexium.elasticsearch5.VertexiumElasticsearchException;
import org.vertexium.elasticsearch5.VertexiumScriptConstants;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class VertexiumElasticsearchPlugin extends Plugin implements SearchPlugin, ScriptPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(
            VertexiumQueryStringQueryBuilder.NAME,
            VertexiumQueryStringQueryBuilder::new,
            parseContext -> (Optional<VertexiumQueryStringQueryBuilder>) (Optional) VertexiumQueryStringQueryBuilder.fromXContent(parseContext)
        ));
    }

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new ScriptEngineService() {
            @Override
            public String getType() {
                return "vertexium";
            }

            @Override
            public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
                VertexiumScriptConstants.ScriptId scriptId = VertexiumScriptConstants.ScriptId.valueOf(scriptName);
                return new ScriptFactory() {
                    @Override
                    public ExecutableScript createExecutableScript(Map<String, Object> vars) {
                        switch (scriptId) {
                            case SAVE_ELEMENT:
                                return new VertexiumSaveElementMutationNativeScript(vars);
                            case SAVE_EXTENDED_DATA:
                                return new VertexiumSaveExtendedDataMutationNativeScript(vars);
                            default:
                                throw new VertexiumElasticsearchException("Unhandled script: " + scriptId);
                        }
                    }

                    @Override
                    public SearchScript createSearchScript(SearchLookup lookup, Map<String, Object> vars) {
                        switch (scriptId) {
                            case LENGTH_OF_STRING:
                                return new LengthOfStringScript(lookup, vars);
                            case CALENDAR_FIELD_AGGREGATION:
                                return new CalendarFieldAggregationScript(lookup, vars);
                            case SORT:
                                return new SortScript(lookup, vars);
                            default:
                                throw new VertexiumElasticsearchException("Unhandled script: " + scriptId);
                        }
                    }
                };
            }

            @Override
            public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
                ScriptFactory scriptFactory = (ScriptFactory) compiledScript.compiled();
                return scriptFactory.createExecutableScript(vars);
            }

            @Override
            public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
                ScriptFactory scriptFactory = (ScriptFactory) compiledScript.compiled();
                return scriptFactory.createSearchScript(lookup, vars);
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    interface ScriptFactory {
        ExecutableScript createExecutableScript(Map<String, Object> vars);

        SearchScript createSearchScript(SearchLookup lookup, Map<String, Object> vars);
    }
}
