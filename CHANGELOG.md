# v2.0.4

* Changed `ValueSerializer` to `VertexiumSerializer`. This requires a change to your configuration:
  `accumulo.graph.valueSerializer` is now `accumulo.graph.serializer` and the class name specified should
  change as well.
* get multiple metadata entry values helper methods `Metadata.getEntries` and `Metadata.getValues`
* CLI improvements
  * delete and query shortcuts
  * additional help
  * Upgrade groovy to 2.4.5
  * Upgrade jline to 2.13
* Elasticsearch
  * optionally run Elasticsearch in process (i.e. no ES server needed)
  * indexes in and out vertex ids
  * vertex query not filtering edges (issue #41)
* `AccumuloGraph.findRelatedEdgeSummary` bug fix to filter out soft deleted and hidden edges
* `InMemoryGraph` bug fixes

# v2.0.3

* statistics aggregation support
* aggregation bug fixes
