# v4.1.0
* Added: Edge query support for in and out vertex ids
* Changed: If fetch hints were not provided throw an exception instead of returning null
* Changed: Elasticsearch to use BulkProcessor 
* Changed: Calling hasId multiple times on the same Query object will result in the intersection of the provided lists. Previously, the resulting list was a union of all provided inputs. This behavior is more consistent with the `AND` behavior of the other query methods.
* Fixed: Updating metadata when metadata was not fetched in fetch hints

# v4.0.0
* Changed: FetchHints to support more filtering
* Changed: Throw errors when calling methods without the proper fetch hints
* Changed: Accumulo: Store metadata in an indexed list to prevent duplication in memory and over the wire 
* Fixed: Accumulo Iterator memory leak 
* Added: Method to Vertex to get more detailed edge summary
* Removed: SQL
* Removed: Blueprints and Titan support

# v3.2.3
* Fixed: Not adding all of the geoshape fields to index for existing elements
* Fixed: Expand SPVs when running in memory query strings
* Fixed: Fix query search using different authorizations query string
* Fixed: Fix indexing of geolocation properties
* Fixed: notification to Elasticsearch when properties are added 

# v3.2.2
* Fixed: Improved support for multithreaded clients with InMemoryGraph
* Fixed: Saving BigInteger and BigDecimal when using prepareMutation with existing element

# v3.2.1
* Changed: When saving an ExistingElementMutation, the Elastisearch5Index will now apply the mutations using a painless script rather than making multiple requests.
* Changed: When using Accumulo, all threads now share a single batch writer rather than creating a new writer for every thread. This allows client programs to more effectively use multi-threading.
* Fixed: Prevent concurrent modification exceptions when using InMemoryGraph through synchronizing the methods and also returning Collection copies. 
* Fixed: The InputStream for a StreamingPropertyValue stored in Accumulo now supports mark/reset.
* Fixed: Elasticsearch sort throwing errors for String properties while querying across indices

# v3.2.0
* Changed: Elasticsearch _id and _type fields to be much smaller to minimize the size of the _uid field data cache 
* Changed: Elasticsearch to only refresh the indices that were changed
* Fixed: InMemory and Accumulo fix wrong vertex being returned after re-adding the same vertex with a different visibility after soft delete
* Fixed: Find path when edge labels are deflated
* Fixed: Accumulo stream property value in table data length of reference
* Fixed: Query will now return hidden elements/vertices/edges if FetchHint.INCLUDE_HIDDEN is passed
* Fixed: Query.hasAuthorization will now match elements whose only use of an authorization is that the element is hidden
* Fixed: Query.hasAuthorization will now match elements whose only use of an authorization is a hidden property
* Fixed: Elasticsearch server plugin missing fields while querying across indices
* Fixed: Elasticsearch shard exception if geo property is not defined
* Added: Query methods elementIds/vertexIds/edgeIds are overloaded to accept IdFetchHint, which makes it possible to include ids for hidden elements
* Added: Graph.getExtendedDataInRange in order to bulk load ranges of extended data rows
* Added: SearchIndex.addExtendedData to allow for reindexing extended data
* Added: Elasticsearch exception if any shard failures occur on queries
* Added: Elasticsearch option to force disable the use of the server side plugin 

# v3.1.1
* Added: Graph.getExtendedData to get a single extended data row
* Fixed: InMemory update extended data with different value

# v3.1.0
* Changed: Set Elasticsearch scroll api query to have a size that is the same as page size
* Changed: Properly throw NoSuchElementException from Iterables when next is called with no more elements
* Changed: If exact match is chosen for a property, full text is automatically enabled to support aggregation results 
* Added: Accumulo in table storage of streaming property value data
* Added: Functions to delete extended data columns
* Added: Multi-value extended data columns
* Added: Sorting and aggregating of row ids and table names
* Added: Option to execute a script from the shell
* Fixed: Streaming property value input stream from data table
* Fixed: Elasticsearch5 retry logic to sleep per request failure instead of batch of failures
* Fixed: Mixed case searching on exact match properties
* Fixed: Intermittent bug while indexing streaming property values under high load
* Removed: ElasticSearch 1.x support

# v3.0.4
* Fixed: Infinite loop in PagingIterable if Elasticsearch returned vertices that wasn't saved in the database yet
* Fixed: Pagination returning duplicate ids on different pages by adding default sort by score and then id. If sorts are specified, sort by score and then id after specified sorts. If sorts are not specified, sort by score and then id.
* Fixed: Elasticsearch flush overflow causing excessive exceptions
* Fixed: Fix Elasticsearch5 missing field exception when sorting on multiple indices

# v3.0.3
* Changed: Accumulo default data storage to use in table storage and not overflow to HDFS 
* Added: Added a hasAuthorization method to the Query class to allow searches for any element that uses an authorization string or strings.
* Added: Query extended data on an element
* Added: EmptyResultsGraphQuery and EmptyResultsQueryResultsIterable
* Fixed: GeoPoint.distanceBetween and GeoPoint.calculateCenter calculations
* Fixed: Elasticsearch5 field limit of 1000
* Fixed: Query.hasId to work with extended data rows
* Removed: CompositeGraphQuery

# v3.0.2
* Fixed: Elasticsearch5 throwing unsupported operation exception when adding/updating a property
* Added: Added a has method to the Query class to allow searches on all properties of a particular data type.
* Added: Added a has method to the Query class to allow searches for any elements with/without a property of a particular data type.
* Added: Added a has method to the Query class to allow searches for a value across multiple properties.
* Added: Added a has method to the Query class to allow searches for a elements with a list of properties. The presence of any property in the list will cause the document to match.
* Added: Added a has method to the Query class to allow searches for a elements without a list of properties. The absence of all properties in the list will cause the document to match.
* Added: Implemented support all compare operators for DateOnly field types when using Elasticsearch 5.
* Fixed: Queries with both a query string and aggregations were throwing a "not implemented" exception in the Elasticsearch 5 plugin. 

# v3.0.1
* Changed: Find Path to not return a path that contains one or more vertices that is can't be retrieved because of visibility restrictions
* Changed: Reduced DefaultIndexSelectionStrategy cache load time from 1hr to 5min
* Added: Added a hasId method to the Query class to allow searches to be filtered by element ID.
* Fix: Extended data element type value for edges
* Changed: Field removal from Elasticsearch documents is now queued as a future instead of immediate
* Fix: Marking vertices/edges as hidden will now update the document in the search index as well as the data store
* Fix: Fixed an infinite looping problem in the PagingIterable that may have resulted from a malformed ES document
* Fix: `Elasticsearch5SearchIndex.removeFieldsFromDocument` will not run the removal script in Elasticsearch if there are no properties to remove
* Fix: Precision and error are now configurable when mapping GeoShape properties. This allows an adjustable trade off between index performance and false positive search results.

# v3.0.0
* Changed: Removed ES 2 support and replaced it with ES 5 support
* Changed: Removed support for NameSubstitutionStrategy from the Elasticsearch modules. NameSubstitutionStrategy is still available for Accumulo modules
* Changed: Added property checking when using the `has` method to query the graph for a value. An exception will now be thrown if an attempt is made to either query a property that does not exist or to text query a property that isn't full text indexed
* Note: Upgrading to this version will require re-indexing if you use NameSubstitutionStrategy or are switching to the ES 5 module. 
* Added: Support for extended GeoShape storage and search. Currently the new shapes are only supported by the ElasticSearch 5 module.
* Added: GraphVisitor to visit elements, properties, extended data rows using a visitor pattern
 
# v2.6.2

* Changed: remove deprecated interfaces, examples, and changed Elasticsearch deprecated methods
* Fixed: Contains.NOT_IN not returning vertices the property does not exist in
* Added: Does Not Contain Text Query
* Added: Elasticsearch shard size configuration for term aggregations
* Added: Elasticsearch scroll API support

# v2.6.1

* Fixed: SQL SPV loading when timestamp is out of sync with property
* Fixed: Elasticsearch plugin handling boolean properties in query

# v2.6.0

* Added: ability to query for vertex/edge/element ids rather than the elements themselves
* Added: ability to return if there is any paths between two vertices
* Added: ability to store extended data rows on an element
* Added: Elasticsearch 2.x support
* Added: Cypher query support
* Added: CLI: profiles to allow running the CLI from within an IDE
* Added: hasDirection and hasOtherVertexId to VertexQuery
* Changed: The default behavior of calling methods without fetch hints will use the default fetch hints specified in the graph configuration
* Changed: removed dist module
* Fixed: Issue #135. Passing FetchHint.NONE when retrieving vertices from Accumulo using Elasticsearch will now properly return the vertices rather than an empty Iterable
* Deprecated: EdgeCountScoringStrategy

# v2.5.6

* Fixed: DeleteHistoricalLegacyStreamingPropertyValueData with property keys having common prefix

# v2.5.5

* Changed: Removed timestamp from streaming property value row key. 

# v2.5.4

* Fixed: Find Path max 2 hops not returning one hop paths
* Added: configuration for number of Elasticsearch replicas
* Changed: Delete events added to HistoricalPropertyValue reporting

# v2.5.3

* Added: size to TermsAggregation
* Added: support for Elasticsearch percentile aggregations
* Added: Elasticsearch: Field query string support

# v2.5.2

* Added: support for Elasticsearch range aggregations
* Added: hasChanges method to mutations
* Added: Edge.getVertices which accepts fetch hints
* Added: support to read multiple StreamingPropertyValues with on query
* Added: Graph.findPath exclusion labels
* Added: methods to support deleting multiple properties on a single ES request to minimize collisions.
* Added: support storing history in a separate table
* Changed: avoid saving empty mutations in Graph#saveElementMutations
* Changed: if the Elasticsearch write queue already contains element flush
* Fixed: existing element mutations updating timestamps
* Fixed: bulk request conflict by adding a retry count

# v2.5.1

* Changed: Speed up property reads by using Maps to directly get the property
* Fixed: updated to repopulate the Metadata entriesLock in the case that the class gets deserialized

# v2.5.0

* Added: Optional benchmarks unit test
* Changed: ElasticSearch: asynchronously submit element updates
* Changed: AccumuloGraph to use MultiTableBatchWriter
* Changed: org.vertexium.Metadata accesses are now protected with ReadWriteLock
* Fixed: More checks for null vertex ids or labels when creating edges
* Changed: Upgrade elasticsearch version to 1.7.5
* Changed: Update accumulo version to 1.7.2
* Added: getVertices helper method on Edge
* Added: graph.saveElementMutations

# v2.4.6

* Changed: Delete events added to HistoricalPropertyValue reporting
* Add test and javadoc for altering property that has a property key that is not the default

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
