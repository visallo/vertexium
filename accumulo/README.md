
Implementation details
----------------------

The graph is stored in a hand full of tables with the following schema.

### data

<table>
<tr><th>Row</th>                                                                  <th>CF</th>         <th>CQ</th>              <th>Value</th>       <th>Description</th></tr>
<tr><td>D[V/E[id]]\x1f[propertyName]\x1f[propertyKey]\x1f[propertyTimestamp]</td> <td>-</td>          <td>-</td>               <td>data</td>        <td>Stores the data for StreamingPropertyValue</td></tr>
</table>

### vertex

<table>
<tr><th>Row</th>                                          <th>CF</th>         <th>CQ</th>                                            <th>Value</th>           <th>Description</th></tr>
<tr><td>V[id]</td>                                        <td>V</td>          <td>-</td>                                             <td>-</td>               <td>Vertex id</td></tr>
<tr><td>V[id]</td>                                        <td>H</td>          <td>H</td>                                             <td>-</td>               <td>Vertex hidden marker</td></tr>
<tr><td>V[id]</td>                                        <td>D</td>          <td>D</td>                                             <td>-</td>               <td>Vertex soft delete marker</td></tr>
<tr><td>V[id]</td>                                        <td>EOUT</td>       <td>[e id]</td>                                        <td>[e label]</td>       <td>Vertex out-edge</td></tr>
<tr><td>V[id]</td>                                        <td>EOUTH</td>      <td>[e id]</td>                                        <td>-</td>               <td>Vertex out-edge hidden marker</td></tr>
<tr><td>V[id]</td>                                        <td>EOUTD</td>      <td>[e id]</td>                                        <td>-</td>               <td>Vertex out-edge soft delete marker</td></tr>
<tr><td>V[id]</td>                                        <td>EIN</td>        <td>[e id]</td>                                        <td>[e label]</td>       <td>Vertex in-edge</td></tr>
<tr><td>V[id]</td>                                        <td>EINH</td>       <td>[e id]</td>                                        <td>-</td>               <td>Vertex in-edge hidden marker</td></tr>
<tr><td>V[id]</td>                                        <td>EIND</td>       <td>[e id]</td>                                        <td>-</td>               <td>Vertex in-edge soft delete marker</td></tr>
<tr><td>V[id]</td>                                        <td>VOUT</td>       <td>[v id]</td>                                        <td>[e label]</td>       <td>Vertex on other side of out-edge</td></tr>
<tr><td>V[id]</td>                                        <td>VIN</td>        <td>[v id]</td>                                        <td>[e label]</td>       <td>Vertex on other side of in-edge</td></tr>
<tr><td>V[id]</td>                                        <td>PROP</td>       <td>[pname\x1fpkey]</td>                               <td>[pval]</td>          <td>Element property</td></tr>
<tr><td>V[id]</td>                                        <td>PROPD</td>      <td>[pname\x1fpkey]</td>                               <td>-</td>               <td>Element property delete marker</td></tr>
<tr><td>V[id]</td>                                        <td>PROPH</td>      <td>[pname\x1fpkey\x1fpvisibility]</td>                <td>-</td>               <td>Element property hidden marker</td></tr>
<tr><td>V[id]</td>                                        <td>PROPMETA</td>   <td>[pname\x1fpkey\x1fpvisibility\x1fmetadataKey]</td> <td>[metadataValue]</td> <td>Element property metadata</td></tr>
</table>

### edge

<table>
<tr><th>Row</th>                                          <th>CF</th>         <th>CQ</th>                                            <th>Value</th>           <th>Description</th></tr>
<tr><td>E[id]</td>                                        <td>E</td>          <td>[e label]</td>                                     <td>-</td>               <td>Edge id</td></tr>
<tr><td>E[id]</td>                                        <td>H</td>          <td>H</td>                                             <td>-</td>               <td>Edge hidden marker</td></tr>
<tr><td>E[id]</td>                                        <td>D</td>          <td>D</td>                                             <td>-</td>               <td>Edge soft delete marker</td></tr>
<tr><td>E[id]</td>                                        <td>VOUT</td>       <td>[v id]</td>                                        <td>-</td>               <td>Edge out-vertex</td></tr>
<tr><td>E[id]</td>                                        <td>VIN</td>        <td>[v id]</td>                                        <td>-</td>               <td>Edge in-vertex</td></tr>
<tr><td>E[id]</td>                                        <td>PROP</td>       <td>[pname\x1fpkey]</td>                               <td>[pval]</td>          <td>Element property</td></tr>
<tr><td>E[id]</td>                                        <td>PROPD</td>      <td>[pname\x1fpkey]</td>                               <td>-</td>               <td>Element property soft delete marker</td></tr>
<tr><td>E[id]</td>                                        <td>PROPH</td>      <td>[pname\x1fpkey\x1fpvisibility]</td>                <td>-</td>               <td>Element property hidden marker</td></tr>
<tr><td>E[id]</td>                                        <td>PROPMETA</td>   <td>[pname\x1fpkey\x1fpvisibility\x1fmetadataKey]</td> <td>[metadataValue]</td> <td>Element property metadata</td></tr>
</table>

StreamingPropertyValue
----------------------

Streaming property values are stored as references in the vertex and edge table using StreamingPropertyValueRef. 
If the data is less than "maxStreamingPropertyValueTableDataSize" (default: 10MB) the data is stored as a row
in the data table. If the data is larger than "maxStreamingPropertyValueTableDataSize" the data is stored in
HDFS (see hdfs.dataDir configuration property).
