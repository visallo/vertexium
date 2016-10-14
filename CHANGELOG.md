# v2.5.0

* Added: Optional benchmarks unit test
* Changed: ElasticSearch: asynchronously submit element updates
* Changed: AccumuloGraph to use MultiTableBatchWriter
* Changed: org.vertexium.Metadata accesses are now protected with ReadWriteLock
* Fixed: More checks for null vertex ids or labels when creating edges
* Changed: Upgrade elasticsearch version to 1.7.5
* Changed: Update accumulo version to 1.7.2
* Added: getVertices helper method on Edge

# v2.4.5

* Added: Vertex.query limited by connected edge labels
* Fixed: Memory leak when using compression in serializer
* Speed up Accumulo EdgeInfo and fix null conditions
* Add validation for edge in/out vertices and label

# v2.4.4

* Quick Kryo Serializer: add support to compress the bytes after serialization
* InMemory/SQL: Fix historical metadata values getting lost on change of metadata
* Graph: Remove the comparison of graph in the GraphEvent base class
* Added interface GraphWithSearchIndex, which GraphBaseWithSearchIndex now implements

# v2.4.3

* Accumulo: fix accumulo not cleaning up property with old visibility in search index

# v2.4.2

* Accumulo/Blueprints: Exclude ripple-flow-rdf from dependencies because of transitive dependency issues
* maven: Update maven plugin versions
* Util: add helper method that makes it a little cleaner to check for an empty Iterable without having to worry
  about closing the iterator.
* InMemory/SQL: update the InMemoryVertex implementations of getVertices and getVertexIds in order to make it
  behave consistently with the Accumulo implementation. When faced with edges to Vertices that can't be seen,
  getVertices will omit them and getVertexIds will return the ids

# v2.4.1

* InMemory/SQL: improved find paths performance
* InMemory/SQL: fix in-memory vertex properties inadvertently sharing metadata
* Graph: remove excessive warning when scanning all elements.
* Graph: add methods to get vertices and edges in a range of IDs
* Accumulo: add methods to get the tablet splits
* Elasticsearch: Add extended bounds to histograph aggregation

# v2.4.0

* ACCUMULO: iterator locations in accumulo config are now stored per table name
* Elasticsearch: fix: sort by strings with tokens should not effect sort order 
* SQL: Switch to HikariCP connection pool

# v2.3.1

* Elasticsearch: fix: calendar date field aggregations with multiple visibilities
* Elasticsearch: Support additional configuration for in process node
* Elasticsearch: fix: Aggregation after alter visibility
* SQL: fix: refresh in memory representation after altering vertex visibility

# v2.3.0

* SQL: index vertex columns on edge table
* Elasticsearch: Load field mapping on startup
* Search: Add CalendarFieldAggregation
* StreamingPropertyValue: Add `readToString` method with offset and limit

# v2.2.12

* Elasticsearch: only sort on properties know to be in the index
* fix inmemory implementation to handle soft deleting with IndexHint of DO NOT INDEX
* close SPV input stream in readToString

# v2.2.11

* SQL: Change max primary key in DDL to 767 to fix MySQL
* Elasticsearch: use an inline Groovy script to efficiently delete a property; also fixes query bugs
* ACCUMULO: fix `markPropertyVisible` for edge properties
* ACCUMULO: fix saving mutation with the save key/name/visibility properties with different timestamps

# v2.2.10

* ACCUMULO: fix find path traversing over deleted edges
* InMemory: add `hashCode()` to `InMemoryElement`

# v2.2.9

* SQL: change varchar size in create tables from 100 to 4000

# v2.2.8

* SQL: create tables if they do not exist
* add method to get all historical values of all properties of an element

# v2.2.7

* Elasticsearch: disable in-process zen discovery by default

# v2.2.6

* allow overriding ES configuration
* for AccumuloGraph, when altering an element visibility also add the element to the search index with the new visibility
* fix `getLength` in sql streaming value property

# v2.2.5

* add possible configuration to set hadoop conf dir

# v2.2.4

* term aggregation is supported in the in memory graph query
* add `serialVersionUID` to serializable SQL classes
* support no default constructors in Kryo
* fix in memory getEdgeIds and getEdgeInfos when getting hidden edges to be consistant with AccumuloGraph

# v2.2.3

* if no matching properties are found for a property return an empty result set. See VertexiumNoMatchingPropertiesException
* for SqlGraph, optimized SQL query used to retrieve vertices
* fix softDeleteProperty on hidden elements
* change the default `search.scoringStrategy` to `org.vertexium.elasticsearch.score.NopScoringStrategy`

# v2.2.2

* fix property definition bug in map reduce code
* add `strictTyping` configuration, defaults to `false` for backwards compatibility
* Use auto deleting file stream if the length of streaming property value is unknown

# v2.2.1

* support Geohash searches
* helper methods on Vertex to get Vertex/Edge pairs
* better ElasticsearchSingleDocumentSearchIndex retry logging
* merged ElasticsearchSingleDocument and ElasticsearchBase
* support edge labels in aggregations

# v2.2.0

* add IP Address type
* support multi-level aggregations
* query bug fixes

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
