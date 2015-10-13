# v2.1.1

* Add IP Address type

# v2.1.0

* Changed `ValueSerializer` to `VertexiumSerializer`. This requires a change to your configuration:
  `accumulo.graph.valueSerializer` is now `accumulo.graph.serializer` and the class name specified should
  change as well.
* get multiple metadata entry values helper methods `Metadata.getEntries` and `Metadata.getValues`
* CLI improvements
  * delete and query shortcuts
  * additional help
  * Upgrade groovy to 2.4.5
  * Upgrade jline to 2.13
* Query
  * Search for multiple edge labels
* Elasticsearch
  * optionally run Elasticsearch in process (i.e. no ES server needed)
  * indexes label, in vertex id and out vertex id
  * vertex query not filtering edges (issue #41)
  * fix hasNot query when no properties are set with that value
  * change the ES query to return based on element visibility not individual property visibilities
* `alterPropertyVisibility` bug fixes
* `AccumuloGraph.findRelatedEdgeSummary` bug fix to filter out soft deleted and hidden edges
* `InMemoryGraph` bug fixes
* `sort` no longer throws exceptions on field not found (issue #47)
* Introduced an experimental `SqlGraph` for using a relational database as an alternative to Accumulo.

# v2.0.3

* statistics aggregation support
* aggregation bug fixes
