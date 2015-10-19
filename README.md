Vertexium [![Build Status](https://travis-ci.org/v5analytics/vertexium.svg?branch=master)](https://travis-ci.org/v5analytics/vertexium)
=========

Vertexium is an API to manipulate graphs, similar to blueprints. Unlike
blueprints, every Vertexium method requires authorizations and visibilities.
Vertexium also supports multivalued properties as well as property metadata.

The Vertexium API was designed to be generic, allowing for multiple implementations.

* Data storage
  * [Accumulo](accumulo/README.md)
  * Experimental: [SQL](sql/README.md)

* Search
  * [Elasticsearch](elasticsearch-singledocument/README.md)

Maven
=====

```
<properties>
    <vertexium.version>0.7.0</vertexium.version>
</properties>
```

```
<dependencies>
    <dependency>
        <groupId>org.vertexium</groupId>
        <artifactId>vertexium-core</artifactId>
        <version>${vertexium.version}</version>
    </dependency>
    <dependency>
        <groupId>org.vertexium</groupId>
        <artifactId>vertexium-inmemory</artifactId>
        <version>${vertexium.version}</version>
    </dependency>
    <dependency>
        <groupId>org.vertexium</groupId>
        <artifactId>vertexium-elasticsearch-singledocument</artifactId>
        <version>${vertexium.version}</version>
    </dependency>
    <dependency>
        <groupId>org.vertexium</groupId>
        <artifactId>vertexium-accumulo</artifactId>
        <version>${vertexium.version}</version>
    </dependency>
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.17</version>
    </dependency>
</dependencies>
```

Accumulo Implementation
=======================

The Accumulo implementation builds on the [cell-level security features](https://accumulo.apache.org/1.5/accumulo_user_manual.html#_security)
to enforce property, edge, and vertex restrictions. This allows the implementation
to enforce security at the tablet server, rather than having to bring the data
back to the application to be sorted out.

Requirements
------------

You'll need a running Accumulo and Elastic Search cluster to try out the Accumulo implementation
of Vertexium. Please see the [Accumulo installation docs](https://accumulo.apache.org/1.5/accumulo_user_manual.html#_installation)
and [Elastic Search setup](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/setup.html)
guide for setting up the respective clusters.

API Usage Examples
------------------

### create and configure an AccumuloGraph instance

```java
import java.util.Map;
import java.util.HashMap;

import org.vertexium.Graph;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;

// specify Accumulo config, more options than shown are available
Map mapConfig = new HashMap();
mapConfig.put(AccumuloGraphConfiguration.USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER, false);
mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, "instance_name");
mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, "username");
mapConfig.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, "password");
mapConfig.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, "localhost");

AccumuloGraphConfiguration graphConfig = new AccumuloGraphConfiguration(mapConfig);
Graph graph = AccumuloGraph.create(graphConfig);
```

### add a vertex

```java
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.accumulo.AccumuloAuthorizations;

// visibility of vertex to be created
Visibility visA = new Visibility("a");

// authorizations of user creating the vertex
Authorizations authA = new AccumuloAuthorizations("a");

Vertex v = graph.addVertex(visA, authA);
```

### add a vertex with properties

```java
Authorizations authA = new AccumuloAuthorizations("a");
Visibility visA = new Visibility("a");
Visibility visB = new Visibility("b");

Vertex v = graph.prepareVertex("v1", visA)
                .setProperty("prop1", "value1", visA)
                .setProperty("prop2", "value2", visB)
                .save(authA);
```

### add an edge

```java
Authorizations authA = new AccumuloAuthorizations("a");
Visibility visA = new Visibility("a");

Vertex v1 = graph.addVertex(visA, authA);
Vertex v2 = graph.addVertex(visA, authA);
Edge e = graph.addEdge(v1, v2, "label1", visA, authA);
```

### get all vertex edges

```java
import org.vertexium.Direction;
import org.vertexium.Edge;

Authorizations authA = new AccumuloAuthorizations("a");
Vertex v1 = graph.getVertex("v1", authA);
Iterable<Edge> edges = v1.getEdges(Direction.BOTH, authA);
```

### full-text vertex search

```java
Authorizations authA = new AccumuloAuthorizations("a");
Iterable<Vertex> vertices = graph.query("vertex", authA).vertices();
```

Configuration
-------------

The Accumulo implementation has quite a few configuration properties, all with
defaults. Please see the `public static final String` fields in
[org.vertexium.accumulo.AccumuloGraphConfiguration](vertexium-accumulo/src/main/java/org/neolumin/vertexium/accumulo/AccumuloGraphConfiguration.java?source=c#L29) for a full
listing.

Iterators
------------------
The Accumulo implementation of Vertexium can make use of server-side iterators
to improve performance by limiting rows returned by tablet servers to only those
where the end user has the proper authorizations. This requires copying the
`vertexium-accumulo-iterators-*.jar` file to `$ACCUMULO_HOME/lib/ext` on each
Accumulo server. Use `mvn package` to build the required JAR file.

Status
======

Vertexium is an actively developed and maintained project that should be
considered to be in a beta state. You should not expect to find significant
bugs or missing functionality in the Accumulo implementation, but the API is
still changing. Please keep that in mind if you decide to use Vertexium.

Contributing
============

We welcome and encourage participation and contribution from anyone interested
in using Vertexium. Please see our [contributing guide](https://github.com/v5analytics/vertexium/blob/master/CONTRIBUTING.md)
to better understand how you can pitch in.

License
=======

Copyright 2014 V5 Analytics

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

