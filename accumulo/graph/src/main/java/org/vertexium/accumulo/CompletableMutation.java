package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;

import java.util.concurrent.CompletableFuture;

public class CompletableMutation extends Mutation {
    private final CompletableFuture<Void> future;

    public CompletableMutation(Mutation m) {
        super(m);
        future = m instanceof CompletableMutation ? ((CompletableMutation) m).future : new CompletableFuture<>();
    }

    public CompletableMutation(CharSequence row) {
        super(row);
        future = new CompletableFuture<>();
    }

    public CompletableMutation(Text row) {
        super(row);
        future = new CompletableFuture<>();
    }

    public CompletableFuture<Void> getFuture() {
        return future;
    }
}
