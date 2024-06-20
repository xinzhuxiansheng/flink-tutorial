package com.yzhou.blog.wordcount.checkpoint;

import java.io.IOException;

@FunctionalInterface
public interface KeyGroupElementsConsumer<T> {
    void consume(T element, int keyGroupId) throws IOException;
}