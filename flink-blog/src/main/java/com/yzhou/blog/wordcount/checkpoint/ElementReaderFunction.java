package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

@FunctionalInterface
public interface ElementReaderFunction<T> {
    T readElement(DataInputView in) throws IOException;
}
