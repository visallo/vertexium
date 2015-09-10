
Setting up Gremlin command line interface
-----------------------------------------

1. [Download](https://github.com/tinkerpop/gremlin/wiki/Downloads) and extract Gremlin (tested with 2.6.0)
1. Create a file called `gremlin-vertexium-accumulo.config` with the following contents  (change where applicable):

        storage=org.vertexium.accumulo.blueprints.AccumuloVertexiumBlueprintsGraphFactory
        storage.graph.tableNamePrefix=vertexium
        storage.graph.accumuloInstanceName=vertexium
        storage.graph.zookeeperServers=localhost
        storage.graph.username=root
        storage.graph.password=password
        storage.graph.autoFlush=true

        storage.graph.search=org.vertexium.elasticsearch.ElasticsearchSingleDocumentSearchIndex
        storage.graph.search.locations=localhost
        storage.graph.search.indexName=vertexium

        storage.graph.serializer=org.vertexium.accumulo.serializer.JavaValueSerializer

        storage.graph.idgenerator=org.vertexium.id.UUIDIdGenerator

        storage.graph.visibilityProvider=org.vertexium.blueprints.DefaultVisibilityProvider

        storage.graph.authorizationsProvider=org.vertexium.accumulo.blueprints.AccumuloAuthorizationsProvider
        storage.graph.authorizationsProvider.auths=auth1,auth2

1. Create a file called `gremlin-vertexium.script` with the following contents:

        g = org.vertexium.blueprints.VertexiumBlueprintsFactory.open('gremlin-vertexium-accumulo.config')

1. From the root of vertexium `mvn package -DskipTests`.
1. Copy from vertexium `dist/target/vertexium-dist-*/lib/*` to `${GREMLIN_HOME}/lib`
1. Delete the older lucene jar in gremlin lib directory `lucene-core-3.6.2.jar`
1. Run `${GREMLIN_HOME}/bin/gremlin.sh gremlin-vertexium.script`
1. Test it out:
        
        v = g.addVertex()
        g.V

Setting up Rexster
------------------

1. Download Rexster and unzip

        curl -O -L http://tinkerpop.com/downloads/rexster/rexster-server-2.4.0.zip > rexster-server-2.4.0.zip
        unzip rexster-server-2.4.0.zip

1. From the root of vertexium `mvn package -DskipTests`.
1. Copy from vertexium `dist/target/vertexium-dist-*/lib/*` to `${REXSTER_HOME}/lib`
1. Delete the older lucene jar in gremlin lib directory `lucene-core-3.6.2.jar`
1. Edit `${REXSTER_HOME}/config/rexster.xml` and add the following to the graphs element (change where applicable):

        <graph>
            <graph-name>vertexium</graph-name>
            <graph-type>org.vertexium.accumulo.blueprints.AccumuloVertexiumRexsterGraphConfiguration</graph-type>
            <storage>org.vertexium.accumulo.blueprints.AccumuloVertexiumBlueprintsGraphFactory</storage>
            <graph-accumuloInstanceName>accumulo</graph-accumuloInstanceName>
            <graph-username>root</graph-username>
            <graph-password>password</graph-password>
            <graph-tableNamePrefix>vertexium</graph-tableNamePrefix>
            <graph-zookeeperServers>192.168.33.10,192.168.33.10</graph-zookeeperServers>
            <graph-serializer>org.vertexium.accumulo.serializer.JavaValueSerializer</graph-serializer>
            <graph-idgenerator>org.vertexium.id.UUIDIdGenerator</graph-idgenerator>
            <graph-search>org.vertexium.elasticsearch.ElasticSearchSearchIndex</graph-search>
            <graph-search-locations>192.168.33.10</graph-search-locations>
            <graph-search-indexName>vertexium</graph-search-indexName>
            <graph-visibilityProvider>org.vertexium.blueprints.DefaultVisibilityProvider</graph-visibilityProvider>
            <graph-authorizationsProvider>org.vertexium.accumulo.blueprints.AccumuloAuthorizationsProvider</graph-authorizationsProvider>
            <graph-authorizationsProvider-auths>auth1,auth2</graph-authorizationsProvider-auths>
            <extensions>
                <allows>
                    <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>

1. Run `${REXSTER_HOME}/bin/rexster.sh -s`
1. Browse to `http://localhost:8182/`
