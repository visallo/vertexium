package org.vertexium;

import java.util.concurrent.CompletableFuture;

public class SaveResult<T extends Element> extends CompletableFuture<T> {
    private CompletableFuture<T> elementReadyFuture;

    public SaveResult(T element) {
        elementReadyFuture = CompletableFuture.completedFuture(element);
    }

    public static <T extends Element> SaveResult<T> completed(T element) {
        SaveResult<T> result = new SaveResult<>(element);
        result.complete(element);
        return result;
    }

    public CompletableFuture<T> getElementReadyFuture() {
        return elementReadyFuture;
    }

    public void complete() {
        if (!elementReadyFuture.isDone()) {
            throw new VertexiumException("elementReadyFuture must be called and completed before calling complete");
        }
        super.complete(elementReadyFuture.join());
    }
}
