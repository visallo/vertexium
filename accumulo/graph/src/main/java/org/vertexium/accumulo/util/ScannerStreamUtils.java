package org.vertexium.accumulo.util;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.vertexium.util.DelegatingStream;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ScannerStreamUtils {
    public static Stream<Map.Entry<Key, Value>> stream(ScannerBase scanner) {
        Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
        AtomicBoolean scannerClosed = new AtomicBoolean();
        Runnable closeScanner = () -> {
            if (!scannerClosed.get()) {
                scannerClosed.set(true);
                scanner.close();
            }
        };
        Stream<Map.Entry<Key, Value>> stream = StreamSupport.stream(new Spliterator<Map.Entry<Key, Value>>() {
            @Override
            public boolean tryAdvance(Consumer<? super Map.Entry<Key, Value>> action) {
                if (scannerClosed.get()) {
                    return false;
                }
                if (it.hasNext()) {
                    Map.Entry<Key, Value> row = it.next();
                    action.accept(row);
                    return true;
                }
                closeScanner.run();
                return false;
            }

            @Override
            public Spliterator<Map.Entry<Key, Value>> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.ORDERED;
            }
        }, false);
        return new DelegatingStream<Map.Entry<Key, Value>>(stream) {
            @Override
            public void close() {
                closeScanner.run();
                super.close();
            }
        };
    }
}
