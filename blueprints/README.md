
Setting up Gremlin command line interface
-----------------------------------------

1. [Download](https://github.com/tinkerpop/gremlin/wiki/Downloads) and extract Gremlin (tested with 2.6.0)
1. Create a file called `gremlin-sg-accumulo.config` with the following contents:

        storage=AccumuloVertexiumBlueprintsGraphFactory
        storage.graph.useServerSideElementVisibilityRowFilter=false
        storage.graph.tableNamePrefix=sg
        storage.graph.accumuloInstanceName=sg
        storage.graph.zookeeperServers=localhost
        storage.graph.username=root
        storage.graph.password=password
        storage.graph.autoFlush=true

        storage.graph.search=ElasticSearchSearchIndex
        storage.graph.search.locations=localhost
        storage.graph.search.indexName=sg

        storage.graph.serializer=JavaValueSerializer

        storage.graph.idgenerator=UUIDIdGenerator

        storage.visibilityProvider=DefaultVisibilityProvider

        storage.authorizationsProvider=AccumuloAuthorizationsProvider
        storage.authorizationsProvider.auths=auth1,auth2

1. Create a file called `gremlin-sg.script` with the following contents:

        g = VertexiumBlueprintsFactory.open('gremlin-sg-accumulo.config')

1. Run `mvn package -DskipTests` from the root of vertexium.
1. Run

        cp vertexium-core/target/vertexium-core-*.jar ${GREMLIN_HOME}/lib
        cp vertexium-blueprints/target/vertexium-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp vertexium-accumulo/target/vertexium-accumulo-*.jar ${GREMLIN_HOME}/lib
        cp vertexium-accumulo-blueprints/target/vertexium-accumulo-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp vertexium-elasticsearch/target/vertexium-elasticsearch-*.jar ${GREMLIN_HOME}/lib
        cp vertexium-elasticsearch-base/target/vertexium-elasticsearch-*.jar ${GREMLIN_HOME}/lib

1. Copy other dependencies accumulo, hadoop, etc. to ${GREMLIN_HOME}/lib

        cp ~/.m2/repository/org/apache/accumulo/accumulo-core/1.5.2/accumulo-core-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/accumulo/accumulo-fate/1.5.2/accumulo-fate-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/accumulo/accumulo-trace/1.5.2/accumulo-trace-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-client/0.23.10/hadoop-client-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-common/0.23.10/hadoop-common-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-core/0.20.2/hadoop-core-0.20.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-auth/0.23.10/hadoop-auth-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/thrift/libthrift/0.9.0/libthrift-0.9.0.jar ${GREMLIN_HOME}/lib

        cp ~/.m2/repository/org/elasticsearch/elasticsearch/1.2.0/elasticsearch-1.2.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-analyzers-common/4.9.0/lucene-analyzers-common-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-codecs/4.9.0/lucene-codecs-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-core/4.9.0/lucene-core-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-grouping/4.9.0/lucene-grouping-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-highlighter/4.9.0/lucene-highlighter-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-join/4.9.0/lucene-join-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-memory/4.9.0/lucene-memory-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-queries/4.9.0/lucene-queries-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-queryparser/4.9.0/lucene-queryparser-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-sandbox/4.9.0/lucene-sandbox-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-spatial/4.9.0/lucene-spatial-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-suggest/4.9.0/lucene-suggest-4.9.0.jar ${GREMLIN_HOME}/lib

        rm lucene-core-3.6.2.jar

1. Run `${GREMLIN_HOME}/bin/gremlin.sh gremlin-sg.script`
1. Test is out:
        
        v = g.addVertex()
        g.V

Setting up Rexster
------------------

1. Download Rexster and unzip

        curl -O -L http://tinkerpop.com/downloads/rexster/rexster-server-2.4.0.zip > rexster-server-2.4.0.zip
        unzip rexster-server-2.4.0.zip

1. Run maven just like in the gremlin section

1. Copy the Vertexium jars just like in the gremlin section

1. Copy the dependencies just like in the gremlin section to `${REXSTER_HOME}/lib`

1. Edit `${REXSTER_HOME}/config/rexster.xml` and add the following to the graphs element

        <graph>
            <graph-name>vertexium</graph-name>
            <graph-type>AccumuloVertexiumRexsterGraphConfiguration</graph-type>
            <storage>AccumuloVertexiumBlueprintsGraphFactory</storage>
            <graph-useServerSideElementVisibilityRowFilter>false</graph-useServerSideElementVisibilityRowFilter>
            <graph-accumuloInstanceName>accumulo</graph-accumuloInstanceName>
            <graph-username>root</graph-username>
            <graph-password>password</graph-password>
            <graph-tableNamePrefix>sg</graph-tableNamePrefix>
            <graph-zookeeperServers>192.168.33.10,192.168.33.10</graph-zookeeperServers>
            <graph-serializer>JavaValueSerializer</graph-serializer>
            <graph-idgenerator>UUIDIdGenerator</graph-idgenerator>
            <graph-search>ElasticSearchSearchIndex</graph-search>
            <graph-search-locations>192.168.33.10</graph-search-locations>
            <graph-search-indexName>vertexium</graph-search-indexName>
            <visibilityProvider>DefaultVisibilityProvider</visibilityProvider>
            <authorizationsProvider>AccumuloAuthorizationsProvider</authorizationsProvider>
            <authorizationsProvider-auths>auth1,auth2</authorizationsProvider-auths>
            <extensions>
                <allows>
                    <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>
