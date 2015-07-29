package org.vertexium.accumulo;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.vertexium.util.VertexiumLogger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.SortedSet;

public class AccumuloGraphLogger {
    private final VertexiumLogger queryLogger;

    public AccumuloGraphLogger(VertexiumLogger queryLogger) {
        this.queryLogger = queryLogger;
    }

    public void logStartIterator(ScannerBase scanner) {
        if (!queryLogger.isTraceEnabled()) {
            return;
        }

        SortedSet<Column> fetchedColumns = null;
        if (scanner instanceof ScannerOptions) {
            fetchedColumns = ((ScannerOptions) scanner).getFetchedColumns();
        }

        String table = null;
        try {
            Field tableField = scanner.getClass().getDeclaredField("table");
            tableField.setAccessible(true);
            Object tableObj = tableField.get(scanner);
            if (tableObj instanceof String) {
                table = (String) tableObj;
            } else {
                table = tableObj.toString();
            }
        } catch (Exception e) {
            queryLogger.trace("Could not get table name from scanner", e);
        }

        if (scanner instanceof BatchScanner) {
            try {
                Field rangesField = scanner.getClass().getDeclaredField("ranges");
                rangesField.setAccessible(true);
                ArrayList<Range> ranges = (ArrayList<Range>) rangesField.get(scanner);
                if (ranges.size() == 0) {
                    logStartIterator(table, (Range) null, fetchedColumns);
                } else if (ranges.size() == 1) {
                    logStartIterator(table, ranges.iterator().next(), fetchedColumns);
                } else {
                    logStartIterator(table, ranges, fetchedColumns);
                }
            } catch (Exception e) {
                queryLogger.trace("Could not get ranges from BatchScanner", e);
            }
        } else if (scanner instanceof Scanner) {
            Range range = ((Scanner) scanner).getRange();
            logStartIterator(table, range, fetchedColumns);
        } else {
            queryLogger.trace("begin accumulo iterator: %s", scanner.getClass().getName());
        }
    }

    private void logStartIterator(String table, Range range, SortedSet<Column> fetchedColumns) {
        String fetchedColumnsString = fetchedColumnsToString(fetchedColumns);
        if (range == null || (range.getStartKey() == null && range.getEndKey() == null)) {
            queryLogger.trace("begin accumulo iterator %s: (%s): all items", table, fetchedColumnsString);
        } else {
            queryLogger.trace("begin accumulo iterator %s: (%s): %s - %s", table, fetchedColumnsString, keyToString(range.getStartKey()), keyToString(range.getEndKey()));
        }
    }

    private void logStartIterator(String table, ArrayList<Range> ranges, SortedSet<Column> fetchedColumns) {
        String fetchedColumnsString = fetchedColumnsToString(fetchedColumns);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Range r : ranges) {
            if (!first) {
                sb.append("\n");
            }
            sb.append("  ").append(keyToString(r.getStartKey())).append(" - ").append(keyToString(r.getEndKey()));
            first = false;
        }
        queryLogger.trace("begin accumulo iterator %s: (%s):\n%s", table, fetchedColumnsString, sb.toString());
    }

    private String keyToString(Key key) {
        StringBuilder sb = new StringBuilder();
        appendText(sb, key.getRow());
        if (key.getColumnFamily() != null && key.getColumnFamily().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnFamily());
        }
        if (key.getColumnQualifier() != null && key.getColumnQualifier().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnQualifier());
        }
        if (key.getColumnVisibility() != null && key.getColumnVisibility().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnVisibility());
        }
        if (key.getTimestamp() != Long.MAX_VALUE) {
            sb.append(":");
            sb.append(key.getTimestamp());
        }
        return sb.toString();
    }

    private String fetchedColumnsToString(SortedSet<Column> fetchedColumns) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Column fetchedColumn : fetchedColumns) {
            if (!first) {
                sb.append(",");
            }
            sb.append(fetchedColumnToString(fetchedColumn));
            first = false;
        }
        return sb.toString();
    }

    private String fetchedColumnToString(Column fetchedColumn) {
        StringBuilder sb = new StringBuilder();
        appendBytes(sb, fetchedColumn.getColumnFamily());
        if (fetchedColumn.getColumnQualifier() != null) {
            sb.append(":");
            appendBytes(sb, fetchedColumn.getColumnQualifier());
        }
        if (fetchedColumn.getColumnVisibility() != null) {
            sb.append(":");
            appendBytes(sb, fetchedColumn.getColumnVisibility());
        }
        return sb.toString();
    }

    private void appendText(StringBuilder sb, Text text) {
        String str = text.toString();
        for (char c : str.toCharArray()) {
            if (c >= ' ' && c <= '~') {
                sb.append(c);
            } else {
                sb.append("\\x");
                String hexString = "00" + Integer.toHexString((int) c);
                sb.append(hexString.substring(hexString.length() - 2));
            }
        }
    }

    private void appendBytes(StringBuilder sb, byte[] bytes) {
        sb.append(new String(bytes));
    }

    public void logEndIterator(long time) {
        queryLogger.debug("accumulo iterator closed (time %dms)", time);
    }
}
