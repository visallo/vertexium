package org.vertexium.accumulo.util;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NamingThreadFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

// Based on org.apache.accumulo.core.client.impl.TabletServerBatchReader
public class VisalloTabletServerBatchReader extends ScannerOptions implements BatchScanner {
    private final String tableId;
    private final int numThreads;
    private final ClientContext context;
    private final Authorizations authorizations;
    private ArrayList<Range> ranges;

    private static final ExecutorService queryThreadPool = new ThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        30L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new NamingThreadFactory("Accumulo batch scanner read ahead thread")
    );

    public VisalloTabletServerBatchReader(
        Connector connector,
        String tableName,
        Authorizations authorizations,
        int numQueryThreads
    ) throws TableNotFoundException {
        ClientContext context = ConnectorUtils.getContext(connector);
        String tableId = Tables.getTableId(connector.getInstance(), tableName);

        checkArgument(context != null, "context is null");
        checkArgument(tableId != null, "tableId is null");
        checkArgument(authorizations != null, "authorizations is null");
        this.context = context;
        this.authorizations = authorizations;
        this.tableId = tableId;
        this.numThreads = numQueryThreads;
        this.ranges = null;
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.size() == 0) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }
        this.ranges = new ArrayList<>(ranges);
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> iterator() {
        if (ranges == null) {
            throw new IllegalStateException("ranges not set");
        }

        return new TabletServerBatchReaderIterator(
            context,
            tableId,
            authorizations,
            ranges,
            numThreads,
            queryThreadPool,
            this,
            timeOut
        );
    }
}
